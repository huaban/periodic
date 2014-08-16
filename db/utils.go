package db


import (
    "reflect"
    "strings"
)

func GetTableName(obj interface{}) string {
    typeof := reflect.TypeOf(obj)
    tableName := strings.ToLower(typeof.Name())
    return tableName
}
