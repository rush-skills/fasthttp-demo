### Demo Golang Project

This project uses FastHTTP to serve an API route, that consumes a json in the format `{"text": "hello world", "content_id": {{content_id}}, "client_id": {{client_id}}, "timestamp": "2020/04/17 05:26:26.431"}
` and streams it to a file on s3 in the format `chat/{{date}}/content_logs_{{date}}_{{client_id}}`

#### Installation

- Clone the repo
- Install Go 
- Install Dependencies (`go get -u ./`)

#### Build

> make all

#### Running

- `bin/api` for the api web server
- `bin/blaster` for the load tester