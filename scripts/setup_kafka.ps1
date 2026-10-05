$ErrorActionPreference = "Stop"

$container = "rtsed-kafka"
$bootstrap = "localhost:9092"
$topics = @("stock_data", "stock_events")

Write-Host "Checking Kafka container..."
docker ps --format "{{.Names}}" | Select-String -SimpleMatch $container | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Kafka container '$container' is not running. Run: docker compose up -d"
}

foreach ($topic in $topics) {
    Write-Host "Creating/verifying topic: $topic"
    docker exec $container /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server $bootstrap `
        --create `
        --if-not-exists `
        --topic $topic `
        --partitions 3 `
        --replication-factor 1
}

Write-Host ""
Write-Host "Kafka topics:"
docker exec $container /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server $bootstrap `
    --list

Write-Host ""
Write-Host "Topic descriptions:"
foreach ($topic in $topics) {
    docker exec $container /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server $bootstrap `
        --describe `
        --topic $topic
}
