# Setup
Start vscode in the devcontainer or
run:
```bash
cd .devcontainer
docker compose up -d postgres
```

# Try

Run:
```bash
go run main.go
```

In a second terminal run:
```bash
./post.sh
```

See the output from main.go to see the queue and notifications working.

# Stop
Interrupt the go process or run the script:
```bash
./stop.sh
```
