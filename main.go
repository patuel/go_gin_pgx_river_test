package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type teststruct struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Address string `json:"address"`
}

var data = []teststruct{
	{ID: 1, Name: "foo", Address: "bar"},
}

func getData(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, data)
}

func main() {
	router := gin.Default()
	router.GET("/data", getData)

	router.Run("localhost:8080")
}
