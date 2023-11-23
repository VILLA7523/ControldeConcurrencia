package occsharpscheduler

import (
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/orderer/common/blockcutter/scheduler"
	sc "github.com/hyperledger/fabric/orderer/common/blockcutter/scheduler"
	"github.com/hyperledger/fabric/orderer/common/blockcutter/scheduler/common"

	"github.com/hyperledger/fabric/common/flogging" //Registro (logging) de Hyperledger Fabric.
	"github.com/hyperledger/fabric/orderer/common/localconfig"
)

//fltrar mensajes de registros logs en el sistema
var logger = flogging.MustGetLogger("orderer.common.blockcutter.scheduler")

type TxnScheduler struct {
	maxTxnBlkSpan uint64 //Limite máximo de bloques para las transacciones en la estructura TxnScheduler
	debug         bool //Activa ciertos mensajes de depuración

	store            *common.Mvstore
	pendingWriteTxns map[string]common.TxnSet
	pendingReadTxns  map[string]common.TxnSet

	// Los siguientes cuatro campos solo involucran txns que pueden participar en el ciclo
	// la primera clave es la altura del bloque inicial de txns en Bloomfilter
	// la segunda clave es el identificador txn
	// el valor Bloomfilter encapsula todos los txnID que pueden alcanzar el identificador txn

	antiReachables     map[string]*common.RelayedFilter
	txnCommittedHeight map[string]uint64
	succTxns           map[string][]string
	txnAges            *common.TxnPQueue

	graph       *common.Graph     // Solamente para txns pendientes
	txnSnapshot map[string]uint64 // Usado para calcular el intervalo txn
}

func NewTxnScheduler() scheduler.Scheduler {
	// Obtiene la ruta de almacenamiento desde la configuración local.
	storePath := localconfig.MustGetMvStoragePath()
	// Elimina cualquier archivo o directorio existente en la ruta de almacenamiento.
	if err := os.RemoveAll(storePath); err != nil {
		// Si hay un error al eliminar, se lanza una excepción con un mensaje de error.
		panic("Error al eliminar " + storePath + " con mensaje de error: " + err.Error())
	}
	// Devuelve una nueva instancia de TxnScheduler con ciertos parámetros predeterminados.
	return createTxnScheduler(localconfig.GetFSharpTxnSpanLimitWithDefault(), false, storePath)
}

func createTxnScheduler(maxTxnSpan uint64, detailedLog bool, storagePath string) *TxnScheduler {
	// Devuelve una nueva instancia de TxnScheduler con los siguientes campos inicializados.
	return &TxnScheduler{
		// Establece el límite máximo de bloques para transacciones.
		maxTxnBlkSpan: maxTxnSpan,

		// Establece la opción de registro detallado (actualmente está deshabilitada).
		debug: false,

		// Crea una nueva instancia de MvStore (almacenamiento multiversión) usando la ruta de almacenamiento proporcionada.
		store: common.NewMvStore(storagePath),

		// Inicializa mapas para transacciones pendientes de escritura y lectura.
		pendingWriteTxns: make(map[string]common.TxnSet),
		pendingReadTxns:  make(map[string]common.TxnSet),

		// Inicializa mapas y estructuras para el manejo de dependencias y grafos.
		antiReachables:     make(map[string]*common.RelayedFilter),
		txnCommittedHeight: make(map[string]uint64),
		succTxns:           make(map[string][]string),
		txnAges:            common.NewTxnPQueue(),
		graph:              common.NewGraph(),

		// Inicializa un mapa para almacenar instantáneas de transacciones.
		txnSnapshot: make(map[string]uint64),
	}
}


// Posible falso positivo
func (scheduler *TxnScheduler) reachable(fromTxn, toTxn string) bool {
	// Verifica si toTxn está en los anti-reachables, que son registros de transacciones que no pueden participar en un ciclo.
	if antiReachable, ok := scheduler.antiReachables[toTxn]; ok {
		// Devuelve true si fromTxn existe en los anti-reachables de toTxn.
		return antiReachable.Exists(fromTxn)
	}
	// Si no se encuentra, significa que toTxn ha sido eliminado porque ya no puede participar en un ciclo.
	return false
}

// ScheduleTxn programa una transacción en el planificador.
func (scheduler *TxnScheduler) ScheduleTxn(resppayload *peer.ChaincodeAction, nextCommittedHeight uint64, txnID string) bool {
    // Parsea la respuesta del código de cadena para obtener instantáneas de lectura, conjuntos de lectura y conjuntos de escritura.
    readSnapshot, readSets, writeSets := sc.OccParse(resppayload)
    
    // Si no hay conjuntos de lectura, establece la instantánea de lectura en nextCommittedHeight - 1.
    if len(readSets) == 0 {
        readSnapshot = nextCommittedHeight - 1
    }

    // Imprime información sobre la transacción procesada.
    logger.Infof("Procesar mensaje %s: instantánea=%d, comprometido=%d, claves de lectura=%v, claves de escritura=%v", txnID, readSnapshot, nextCommittedHeight, readSets, writeSets)

    // Llama a la función ProcessTxn para procesar la transacción y devuelve el resultado.
    return scheduler.ProcessTxn(readSets, writeSets, readSnapshot, nextCommittedHeight, txnID)
}


func (scheduler *TxnScheduler) ProcessTxn(readSets, writeSets []string, snapshot, nextCommittedHeight uint64,
	txnID string) bool {
	    // Variables para medir el tiempo de ejecución de distintas etapas del procesamiento de la transacción.
		var elapsedResolveDependency, elapsedTestAccessibility, elapsedPending int64 = 0, 0, 0
		var bfsTraversalCounter = 0

		// Estado inicial de la transacción y configuración del registro.
		status := "Schedule"
		defer func(start time.Time) {
			// Se ejecutará después de que la función circundante retorne.
			elapsed := time.Since(start).Nanoseconds() / 1000
			logFunc := logger.Infof
			if status == "DESERT" {
				logFunc = logger.Warnf
			}
			//logFunc("%s Txn %s en %d microsegundos. (Resolver Dependencia: %d us, Probar accesibilidad a través de BFS recorre %d txns: %d us, Actualizar Pendientes: %d us)\n\n", status, txnID, elapsed, elapsedResolveDependency, bfsTraversalCounter, elapsedTestAccessibility, elapsedPending)
		}(time.Now())

		// Verificar si la transacción debe ser descartada debido a límites de bloque.
		if scheduler.maxTxnBlkSpan < nextCommittedHeight && snapshot < nextCommittedHeight-scheduler.maxTxnBlkSpan {
			status = "DESERT"
			return false
		}

		// Iniciar el tiempo de resolución de dependencias.
		startResolveDependency := time.Now()

		// Conjuntos de transacciones para lecturas y anti-escrituras.
		wr := common.NewTxnSet()
		antiRw := common.NewTxnSet()
		antiPendingRw := common.NewTxnSet()

		// Nuevas claves de lectura sin ser sobreescritas antes.
		newReadKeys := make([]string, 0)

		// Filtro para excluir transacciones antiguas.
		filter := func(txn string) bool {
			return scheduler.txnAges.Exists(txn)
		}

		// Procesar conjuntos de lectura.
		for _, readKey := range readSets {
			// Excluyendo el bloque de snapshot,
			// una transacción con snapshot s implica que lee los estados confirmados en s.
			curAntiRw := scheduler.store.UpdatedTxnsNoEarlierThanBlk(snapshot+1, readKey, filter)
			antiRw.InPlaceUnion(curAntiRw)
			if curWr, found := scheduler.store.LastUpdatedTxnNoLaterThanBlk(snapshot, readKey, filter); found {
				wr.Add(curWr)
			}
			existsAntiPendingRw := false
			if curPendingWriteTxns, ok := scheduler.pendingWriteTxns[readKey]; ok {
				antiPendingRw.InPlaceUnion(curPendingWriteTxns)
				existsAntiPendingRw = true
			}
			// Completar NO antiRW transacciones.
			if len(curAntiRw) == 0 && !existsAntiPendingRw {
				newReadKeys = append(newReadKeys, readKey)
			}
		} // fin for readKey


		// Conjuntos de transacciones para escrituras y lecturas escritas.
		ww := common.NewTxnSet()
		rw := common.NewTxnSet()

		// Procesar conjuntos de escritura.
		for _, writeKey := range writeSets {
			// No considerar conflictos de escrituras entre escrituras pendientes,
			// ya que todas son reordenables.
			// Las transacciones de conflicto ww comprometidas pueden no existir,
			// ya que podrían haber sido eliminadas debido a la no concurrencia.
			if curWw, found := scheduler.store.LastUpdatedTxnNoLaterThanBlk(nextCommittedHeight, writeKey, filter); found {
				ww.Add(curWw)
			}

			// Obtener transacciones de lectura anteriores a nextCommittedHeight para la clave de escritura.
			curRw := scheduler.store.ReadTxnsEarlierThanBlk(nextCommittedHeight, writeKey, filter)
			rw.InPlaceUnion(curRw)

			// Agregar transacciones de lectura pendientes para la clave de escritura.
			if curPendingRw, ok := scheduler.pendingReadTxns[writeKey]; ok {
				rw.InPlaceUnion(curPendingRw)
			}
		}

		// Registro de información sobre dependencias de la transacción.
		logger.Infof("Dependencia de Txn %s: wr=%s, ww=%s, rw=%s, anti-rw=%s, pending-anti-rw=%s, claves recién leídas=%v", txnID, wr, ww, rw, antiRw, antiPendingRw, newReadKeys)

		// Medir el tiempo de resolución de dependencias.
		elapsedResolveDependency = time.Since(startResolveDependency).Nanoseconds() / 1000

		// Actualizar la información de accesibilidad y detectar ciclos al mismo tiempo.
		startTestAccessibility := time.Now()
		curPredTxns := common.NewTxnSet()
		curPredTxns.InPlaceUnion(wr)
		curPredTxns.InPlaceUnion(rw)
		curPredTxns.InPlaceUnion(ww)


	// Verificar si hay ciclos en el grafo de dependencias entre transacciones.
	for curSucc := range curSuccTxns {
		for curPred := range curPredTxns {
			if scheduler.reachable(curSucc, curPred) {
				logger.Infof("Ciclo detectado para %s de %s a %s", txnID, curSucc, curPred)
				status = "Abort"
				return false
			}
		}
	}

	// Calcular la duración de la transacción solo para transacciones comprometidas y de lectura/escritura (excluyendo las transacciones de solo escritura)
	if len(readSets) > 0 {
		scheduler.txnSnapshot[txnID] = snapshot
	}

	// Establecer filtros para la detección de transacciones alcanzables.
	scheduler.antiReachables[txnID] = common.CreateRelayedFilter(nextCommittedHeight)
	scheduler.antiReachables[txnID].Add(txnID)

	// Actualizar enlaces de salida para transacciones entrantes.
	for curPredTxn := range curPredTxns {
		// curPredTxn puede ser demasiado antigua para incluirla en el grafo de dependencias.
		if _, ok := scheduler.succTxns[curPredTxn]; ok {
			scheduler.succTxns[curPredTxn] = append(scheduler.succTxns[curPredTxn], txnID)
		}

		if _, ok := scheduler.antiReachables[curPredTxn]; ok {
			scheduler.antiReachables[txnID].Merge(scheduler.antiReachables[curPredTxn])
		}
	}

	// Establecer sucesores de la transacción en el grafo de dependencias.
	scheduler.succTxns[txnID] = curSuccTxns.ToSlice()

	// Unir el conjunto antiAccessible para todas las transacciones durante el recorrido BFS con el de txnID.
	// Actualizar la cola de transacciones en consecuencia.
	bfsTxn := []string{txnID}
	bfsVisited := map[string]bool{}
	for len(bfsTxn) > 0 {
		bfsTraversalCounter++
		curTxn := bfsTxn[0]
		bfsTxn = bfsTxn[1:]
		scheduler.txnAges.Push(curTxn, nextCommittedHeight)
		scheduler.antiReachables[curTxn].Merge(scheduler.antiReachables[txnID])

		// fmt.Printf("curTxn: %s, BFS txns: %v\n", curTxn, bfsTxn)
		for _, outTxn := range scheduler.succTxns[curTxn] {
			if _, exists := bfsVisited[outTxn]; !exists {
				bfsTxn = append(bfsTxn, outTxn)
				bfsVisited[outTxn] = true
			} // end if
		} // end for
	}

	// Medir el tiempo de detección de accesibilidad.
	elapsedTestAccessibility = time.Since(startTestAccessibility).Nanoseconds() / 1000

	// Actualizar los campos relevantes.
	startIndexing := time.Now()
	edgesFrom := []string{}
	edgesTo := []string{}
	for _, pendingTxn := range scheduler.graph.Nodes() {
		if scheduler.reachable(pendingTxn, txnID) && scheduler.reachable(txnID, pendingTxn) {
			// Teóricamente, tal ciclo de transacción es imposible debido a que
			// todas las dependencias de transacciones se han resuelto previamente.
			// Sin embargo, esto es prácticamente posible debido al falso positivo del filtro de Bloom.
			// En este caso, debemos abandonar esta transacción válida del programa, ya que no sabemos en qué dirección de dependencia está el error.
			// Para mayor comodidad, aún conservamos esta transacción en el grafo de dependencias para transacciones posteriores, lo que puede introducir sobrecarga.
			// Este escenario DEBERÍA minimizarse reduciendo la tasa de falsos positivos del filtro de Bloom.
			status = "DESERT"
			return false
		} else if scheduler.reachable(pendingTxn, txnID) {
			edgesFrom = append(edgesFrom, pendingTxn)
			edgesTo = append(edgesTo, txnID)
		} else if scheduler.reachable(txnID, pendingTxn) {
			edgesFrom = append(edgesFrom, txnID)
			edgesTo = append(edgesTo, pendingTxn)
		}
	}

	// Agregar la transacción como un nuevo nodo en el grafo de dependencias.
	if ok := scheduler.graph.AddNode(txnID); !ok {
		logger.Errorf("El nodo %s ya ha sido agregado anteriormente...", txnID)
	}

	// Agregar bordes entre la transacción y otras transacciones en el grafo de dependencias.
	for i := range edgesFrom {
		from := edgesFrom[i]
		to := edgesTo[i]
		if ok := scheduler.graph.AddEdge(from, to); !ok {
			logger.Errorf("El borde %s -> %s ya ha sido agregado anteriormente", from, to)
		} 
	}

	// Agregar la transacción actual a los conjuntos de transacciones pendientes de escritura y lectura.
	for _, writeKey := range writeSets {
		if _, found := scheduler.pendingWriteTxns[writeKey]; !found {
			scheduler.pendingWriteTxns[writeKey] = common.NewTxnSet()
		}
		scheduler.pendingWriteTxns[writeKey].Add(txnID)
	}

	for _, newReadKey := range newReadKeys {
		if _, found := scheduler.pendingReadTxns[newReadKey]; !found {
			scheduler.pendingReadTxns[newReadKey] = common.NewTxnSet()
		}
		scheduler.pendingReadTxns[newReadKey].Add(txnID)
	}

	// Medir el tiempo de indexación.
	elapsedPending = time.Since(startIndexing).Nanoseconds() / 1000
	return true
}


// pruneFilters realiza operaciones de filtrado y ajuste en los conjuntos de datos utilizados por el planificador de transacciones.
// La función se encarga de ajustar los filtros anti-accesibilidad y realizar otras operaciones de limpieza.
func (scheduler *TxnScheduler) pruneFilters(curBlkHeight uint64) {
	// Paso 1: Buscar la transacción más antigua entre todas las transacciones consideradas en el grafo de dependencias.
	var earliestCommittedBlk uint64 = math.MaxUint64
	for _, committedBlk := range scheduler.txnCommittedHeight {
		if committedBlk < earliestCommittedBlk {
			earliestCommittedBlk = committedBlk
		}
	}

	// Paso 2: Rotar los filtros anti-accesibilidad para reflejar el cambio en las alturas de bloque de compromiso.
	for _, antiReachable := range scheduler.antiReachables {
		antiReachable.Rotate(earliestCommittedBlk, curBlkHeight)
	}
}








































































const filterInterval = uint64(40)

func (scheduler *TxnScheduler) ProcessBlk(blkHeight uint64) []string {
	// Variables para medir el tiempo y contadores
	var txnQueueRemovalCount, topoTraversalCounter int = 0, 0
	var elapsedPrunePQueue, elapsedResolvePendingTxn, elapsedTopoTraverseForWw, elapsedStorage, elapsedTopoSort int64 = 0, 0, 0, 0, 0
	avgSpan := 0.0
	
	// Defer se utiliza para ejecutar una función al final de la ejecución de la función circundante.
	// En este caso, se utiliza para imprimir estadísticas después de que la función termine.
	defer func(start time.Time) {
		elapsed := time.Since(start).Nanoseconds() / 1000
		logger.Infof("Recuperar Programación para bloque %d (promedio de espaciado = %.2f) en %d µs.", blkHeight, avgSpan, elapsed)
		logger.Infof("Calcular la programación desde la clasificación topológica en %d µs", elapsedTopoSort)
		logger.Infof("Resolver transacciones pendientes en %d µs", elapsedResolvePendingTxn)
		logger.Infof("Almacenar claves comprometidas en %d µs", elapsedStorage)
		logger.Infof("Traverse Topo %d transacciones para actualizar la dependencia ww en %d µs", topoTraversalCounter, elapsedTopoTraverseForWw)
		logger.Infof("Eliminar %d transacciones no concernientes de TxnPQueue en %d µs", txnQueueRemovalCount, elapsedPrunePQueue)
		logger.Infof("# de transacciones concernientes en la cola: %d", scheduler.txnAges.Size())
	}(time.Now())
	
	// Se ignora las transacciones de instantánea comprometidas que no pueden ser concurrentes
	earliestSnapshot := int64(blkHeight) + 1 - int64(scheduler.maxTxnBlkSpan)
	
	// TODO: Temporalmente no eliminar versiones antiguas de registros, ya que es demasiado costoso iterar todos los registros.
	// La optimización futura puede hacerlo de manera asíncrona.
	// scheduler.store.Clean(uint64(earliestSnapshot))

	start := time.Now()
	var schedule []string
	var ok bool
	// Un ciclo es posible, ya que el borde podría agregarse incorrectamente debido a falsos positivos.
	// Si ocurre un ciclo, eliminar todas las transacciones que participan en el ciclo.
	if schedule, ok = scheduler.graph.Toposort(); !ok {
		panic("Ciclo detectado...")
	}

	// Verificar la validez de la programación si el modo de depuración está activado.
	if scheduler.debug {
		for i, txn1 := range schedule {
			for j, txn2 := range schedule {
				if i < j && scheduler.reachable(txn2, txn1) {
					logger.Infof("Programación: %v", schedule)
					panic("¡Programación inválida!!!!!!" + txn2 + " " + txn1)
				}
			}
		}
	}

	totalTxnblkSpan := uint64(0)
	count := uint64(0)
	schedulePosition := make(map[string]int)
	for idx, txnID := range schedule {
		schedulePosition[txnID] = idx
		if snapshot, ok := scheduler.txnSnapshot[txnID]; ok {
			count++
			totalTxnblkSpan += blkHeight - snapshot
		}

		scheduler.txnCommittedHeight[txnID] = blkHeight
	}
	elapsedTopoSort = time.Since(start).Nanoseconds() / 1000

	// logger.Infof("Iniciar ETAPA %d para Blk %d", 2, committed)
	start = time.Now()
	topoTraverseStartTxns := []string{}
	committedUpdates, committedReads := map[string][]string{}, map[string][]string{}
	for writeKey, pendingTxns := range scheduler.pendingWriteTxns {
		sortedWriteTxns := pendingTxns.ToSlice()
		// Ordenar las transacciones en pendingWrite según el orden relativo en el cronograma.
		sort.Slice(sortedWriteTxns, func(i, j int) bool {
			return schedulePosition[sortedWriteTxns[i]] < schedulePosition[sortedWriteTxns[j]]
		})

		committedUpdates[writeKey] = sortedWriteTxns
		// Esta clave se actualizará en este bloque
		// Estas transacciones no pueden leer los registros recientemente confirmados,
		// ya que el nuevo registro está confirmado.
		delete(scheduler.pendingReadTxns, writeKey)

		if len(sortedWriteTxns) > 1 {
			// Tener en cuenta la dependencia ww en el bloque
			pendingSize := len(sortedWriteTxns)
			findHeadTxn := false
			for i := 1; i < pendingSize; i++ {
				predTxn := sortedWriteTxns[i-1]
				succTxn := sortedWriteTxns[i]
				// Actualizar las transacciones sucesoras del grafo de dependencias aquí para cada ww.
				scheduler.succTxns[predTxn] = append(scheduler.succTxns[predTxn], succTxn)
				if scheduler.reachable(succTxn, predTxn) {
					panic("Desde la clasificación topológica, esta dependencia ww no debería crear un ciclo...")
				}
				// tx1 -ww-> tx2 -ww-> tx3 -ww-> tx4
				// Encontrar la primera transacción sucesora cuya dependencia ww no se ha incorporado en anti-reachable.
				// Actualizar su anti-reachable desde su predecesor y registrar esta transacción sucesora en topoTraverseStartTxns.
				// Más tarde, actualizamos el antiReachable de todas las transacciones accesibles desde topoTraverseStartTxns en una sola iteración.

				// La razón es que supongamos que tx1 -~-> tx2 ya se cumple sin considerar su dependencia ww, no necesitamos considerar este ww para la iteración posterior.
				if !findHeadTxn && !scheduler.reachable(predTxn, succTxn) {
					findHeadTxn = true
					topoTraverseStartTxns = append(topoTraverseStartTxns, succTxn)
					scheduler.antiReachables[succTxn].Merge(scheduler.antiReachables[predTxn])
				}
			} // fin del for
		} // fin del if len
	}
	// Limpiar las pendingWriteTxns
	scheduler.pendingWriteTxns = make(map[string]common.TxnSet)

	for readKey, pendingTxns := range scheduler.pendingReadTxns {
		committedReads[readKey] = pendingTxns.ToSlice()
	}

	scheduler.pendingReadTxns = make(map[string]common.TxnSet)
	elapsedResolvePendingTxn = time.Since(start).Nanoseconds() / 1000

	///////////////////////////////////////////////////////////////////
	// Asociar transacciones y claves en el almacenamiento.
	// logger.Infof("Iniciar ETAPA %d para Blk %d", 4, committed)
	start = time.Now()
	scheduler.store.Commit(blkHeight, committedUpdates, committedReads)
	elapsedStorage = time.Since(start).Nanoseconds() / 1000

	///////////////////////////////////////////////////////////////////
	// Travesía topológica ordenada para transmitir la dependencia ww para la información de accesibilidad.
	// logger.Infof("Iniciar ETAPA %d para Blk %d", 5, committed)
	start = time.Now()
	// Para que todas las dependencias ww puedan considerarse.
	sort.Slice(topoTraverseStartTxns, func(i, j int) bool {
		return schedulePosition[topoTraverseStartTxns[i]] < schedulePosition[topoTraverseStartTxns[j]]
	})
	topoTraversalCounter = scheduler.topoTraverseForWw(topoTraverseStartTxns)
	elapsedTopoTraverseForWw = time.Since(start).Nanoseconds() / 1000
	/////////////////////////////////////////////////////////////////////////////
	// Eliminar transacciones de txnQ que no pueden ser concurrentes (y, por lo tanto, dependientes en reversa)
	// logger.Infof("Iniciar ETAPA %d para Blk %d", 6, committed)
	start = time.Now()
	prevTxn, depHeight, found := scheduler.txnAges.Peek()
	removedTxnDepMap := make(map[string]uint64)
	for found && int64(depHeight) <= earliestSnapshot {
		scheduler.txnAges.Pop()
		removedTxnDepMap[prevTxn] = depHeight

		delete(scheduler.txnCommittedHeight, prevTxn)
		delete(scheduler.succTxns, prevTxn)
		delete(scheduler.antiReachables, prevTxn)

		prevTxn, depHeight, found = scheduler.txnAges.Peek()
		txnQueueRemovalCount++
	}
	elapsedPrunePQueue = time.Since(start).Nanoseconds() / 1000

	if scheduler.debug {
		logger.Infof("Eliminar transacciones de PQueue : %v", removedTxnDepMap)
	}

	/////////////////////////////////////////////////////////////////
	// Crear un nuevo conjunto de filtros para cada bloque de filterInterval
	scheduler.pruneFilters(blkHeight)
	avgSpan = float64(totalTxnblkSpan) / float64(count)
	return schedule
}

func (scheduler *TxnScheduler) topoTraverseForWw(startTxns []string) (counter int) {
	counter = 0
	topoOrder := common.TopoOrder(startTxns, func(from string) []string {
		return scheduler.succTxns[from]
	})
	// Propogate the ww dependency based on the computed topo order
	for _, curTxn := range topoOrder {
		for _, succTxn := range scheduler.succTxns[curTxn] {
			scheduler.antiReachables[succTxn].Merge(scheduler.antiReachables[curTxn])
		} // for outTxn
	} // for i
	counter = len(topoOrder)
	return
}

func (scheduler *TxnScheduler) reportStats(curBlkHeight uint64) {
	// reportStats genera un informe de estadísticas para el planificador de transacciones e imprime información relevante en el registro.
	// Paso 1: Contar las transacciones pendientes de actualización.
	pendingUpdateRecordCount := 0
	pendingUpdateTxnCount := 0
	for _, pendingTxns := range scheduler.pendingWriteTxns {
		pendingUpdateRecordCount++
		pendingUpdateTxnCount += len(pendingTxns)
	}

	// Paso 2: Contar las transacciones pendientes de lectura.
	pendingReadRecordCount := 0
	pendingReadTxnCount := 0
	for _, pendingTxns := range scheduler.pendingReadTxns {
		pendingReadRecordCount++
		pendingReadTxnCount += len(pendingTxns)
	}

	// Paso 3: Obtener el tamaño del grafo de dependencias.
	nodeCount, edgeCount := scheduler.graph.Size()

	// Paso 4: Obtener el número total de transacciones consideradas.
	totalTxnCount := scheduler.txnAges.Size()
	// Paso 5: Imprimir las estadísticas en el registro.
	logger.Infof("Current Blk Height: %d "+
		"pendingUpdate: %d keys, %d txns. "+
		"pendingRead: %d keys, %d txns. "+
		"Graph: %d nodes, %d edges. "+
		"Total Concerned Txns: %d",
		curBlkHeight,
		pendingUpdateRecordCount, pendingUpdateTxnCount,
		pendingReadRecordCount, pendingReadTxnCount,
		nodeCount, edgeCount, totalTxnCount)
}

// detailedStat imprime estadísticas detalladas del planificador de transacciones en la consola.
func (scheduler *TxnScheduler) detailedStat(curBlkHeight uint64) {
	// Imprimir información detallada sobre el estado actual del planificador.
	fmt.Printf("Current Block Height: %d\n", curBlkHeight)
	fmt.Printf("pendingWriteTxns: %v\n", scheduler.pendingWriteTxns)
	fmt.Printf("pendingReadTxns: %v\n", scheduler.pendingReadTxns)
	fmt.Printf("Txn Queue: %v\n", scheduler.txnAges.Raw())
}
