package periodic

import (
    "os"
    "net"
    "log"
)


func sockCheck(sockFile string) {
    _, err := os.Stat(sockFile)
    if err == nil || os.IsExist(err) {
        conn, err := net.Dial("unix", sockFile)
        if err == nil {
            conn.Close()
            log.Fatal("Periodic task system is already started.")
        }
        os.Remove(sockFile)
    }
}
