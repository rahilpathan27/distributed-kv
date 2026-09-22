.PHONY: test run cluster docker-up docker-down

test:
	python3 -m unittest discover -s tests -v

run:
	python3 -m dkv.cli --node-id node1 --port 7070 --data-dir ./data/node1

cluster:
	python3 -m dkv.cli --node-id node1 --port 7070 --data-dir ./data/node1 --peer node2=127.0.0.1:7080 --peer node3=127.0.0.1:7090 & \
	python3 -m dkv.cli --node-id node2 --port 7080 --data-dir ./data/node2 --peer node1=127.0.0.1:7070 --peer node3=127.0.0.1:7090 & \
	python3 -m dkv.cli --node-id node3 --port 7090 --data-dir ./data/node3 --peer node1=127.0.0.1:7070 --peer node2=127.0.0.1:7080

docker-up:
	docker compose -f docker/docker-compose.yml up --build

docker-down:
	docker compose -f docker/docker-compose.yml down
