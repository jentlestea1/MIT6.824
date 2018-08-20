package main
import "net/http"

func main(){
	http.Handle("/", http.FileServer(http.Dir(".")))

    server := &http.Server{
        Addr:    ":4040",
        Handler: http.DefaultServeMux,
    }

    go server.ListenAndServe()
}
