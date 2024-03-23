#!/bin/bash
curl \
    --header "Content-Type: application/json" \
    --request POST \
    --data '{"sender": "XYZ","method": "UP","onb": 2,"transfer_status": "U"}' \
    http://localhost:8080/data