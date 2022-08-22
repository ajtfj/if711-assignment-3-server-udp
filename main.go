package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ajtfj/graph"
)

const (
	GRAPH_FILE         = "graph.txt"
	MAX_DATAGRAMA_SIZE = 1024
)

var (
	g *graph.Graph
)

func HandleUDPRequest(conn *net.UDPConn) {
	for {
		requestJson := make([]byte, MAX_DATAGRAMA_SIZE)
		requestPayload := RequestPayload{}
		n, addr, err := conn.ReadFromUDP(requestJson)
		if err != nil {
			log.Print(err)
			continue
		}
		requestJson = requestJson[:n]
		if err := json.Unmarshal(requestJson, &requestPayload); err != nil {
			encodeError(conn, addr, err)
			continue
		}
		log.Printf("payload received from client %v: %v", addr, requestPayload)

		startTime := time.Now()
		path, err := g.ShortestPath(requestPayload.Ori, requestPayload.Dest)
		if err != nil {
			encodeError(conn, addr, err)
			continue
		}
		duration := time.Since(startTime)

		responsePayload := ResponsePayload{
			Path:         path,
			CalcDuration: duration,
		}
		jsonResponse, err := json.Marshal(responsePayload)
		if err != nil {
			encodeError(conn, addr, err)
			continue
		}
		log.Printf("sending payload to client %v: %v", addr, responsePayload)
		if _, err := conn.WriteTo(jsonResponse, addr); err != nil {
			log.Print(err)
			continue
		}
	}
}

func main() {
	port, ok := os.LookupEnv("PORT")
	if !ok {
		log.Fatal("undefined PORT")
	}

	err := setupGraph()
	if err != nil {
		log.Fatal(err)
	}

	url := fmt.Sprintf("localhost:%s", port)
	addr, err := net.ResolveUDPAddr("udp", url)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal(err)
	}

	defer closeUDPConnection(conn)

	log.Printf("waiting for requests on port %s\n", port)

	for {
		HandleUDPRequest(conn)
	}
}

func parceGraphInputLine(inputLine string) (graph.Node, graph.Node, int, error) {
	matches := strings.Split(inputLine, " ")
	if len(matches) < 3 {
		return graph.Node(""), graph.Node(""), 0, fmt.Errorf("invalid input")
	}

	weight, err := strconv.ParseInt(matches[2], 10, 0)
	if err != nil {
		return graph.Node(""), graph.Node(""), 0, err
	}

	return graph.Node(matches[0]), graph.Node(matches[1]), int(weight), nil
}

func setupGraph() error {
	g = graph.NewGraph()

	file, err := os.Open(GRAPH_FILE)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		inputLine := scanner.Text()
		u, v, weight, err := parceGraphInputLine(inputLine)
		if err != nil {
			return err
		}
		g.AddEdge(u, v, weight)
	}

	return nil
}

type RequestPayload struct {
	Ori  graph.Node `json:"ori"`
	Dest graph.Node `json:"dest"`
}

type ResponsePayload struct {
	Path         []graph.Node  `json:"path"`
	CalcDuration time.Duration `json:"calc-duration"`
}

type ResponseErrorPayload struct {
	Message string `json:"message"`
}

func closeUDPConnection(conn *net.UDPConn) {
	err := conn.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func encodeError(conn *net.UDPConn, addr *net.UDPAddr, err error) {
	payload := ResponseErrorPayload{
		Message: err.Error(),
	}
	jsonResponse, err := json.Marshal(payload)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("sending error to client %v: %s", addr, err.Error())
	if _, err := conn.WriteTo(jsonResponse, addr); err != nil {
		log.Print(err)
		return
	}
}
