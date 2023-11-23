package scheduler

import (
	"strings"

	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
)

type Scheduler interface {
	ScheduleTxn(resppayload *peer.ChaincodeAction, nextCommittedHeight uint64, txnID string) bool
	ProcessBlk(committedHeight uint64) []string
}

func validKey(key string) bool {
	// The keys without the following suffix have special meanings.
	// And should be ignored for reordering.
	if strings.HasSuffix(key, "initialized") {
		// If chaincode is deployed with "--init-required", each txn will read a key ending with "initialized", ignore it for the validation.
		return false
	}
	if localconfig.LineageSupported() && strings.HasSuffix(key, "_prov") {
		return false
	}

	return true
}

func OccParse(resppayload *peer.ChaincodeAction) (readSnapshot uint64, readKeys []string, writeKeys []string) {
    // Crear un conjunto de lectura/escritura de transacciones
    txRWSet := &rwsetutil.TxRwSet{}
    if err := txRWSet.FromProtoBytes(resppayload.Results); err != nil {
        panic("Fallo al recuperar rwset del payload de la transacción")
    }

    // Establecer readSnapshot en 0, si no hay claves de lectura, readSnapshot no es útil. Simplemente configúrelo en la próxima altura de bloque
    readSnapshot = 0

    if 1 < len(txRWSet.NsRwSets) {
        // Para alguna razón, si usamos la antigua API de nodejs para implementar el chaincode,
        //   la acción del chaincode proviene de NsRwSets[0].
        // Si usamos el último comando bash para implementar el chaincode,
        //   la acción del chaincode proviene de NsRwSets[1].
        ns := txRWSet.NsRwSets[1]

        // Recorrer las escrituras y agregar las claves a writeKeys
        for _, write := range ns.KvRwSet.Writes {
            if writeKey := write.GetKey(); validKey(writeKey) {
                writeKeys = append(writeKeys, writeKey)
            }
        }

        // Recorrer las lecturas y agregar las claves a readKeys
        for _, read := range ns.KvRwSet.Reads {
            if readKey := read.GetKey(); validKey(readKey) {
                // Todas las versiones de claves de lectura deben tener el mismo número de bloque, ya que se recuperan de la misma instantánea
                readSnapshot = read.GetVersion().GetBlockNum()
                readKeys = append(readKeys, readKey)
            }
        }
    }

    return
}