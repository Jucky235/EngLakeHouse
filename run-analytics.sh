#!/usr/bin/env bash

for job in spark/jobs/analytics/*.py; do
    echo "========================================"
    echo "Running: $job"
    echo "========================================"

    docker exec lakehouse-spark \
        /opt/spark/bin/spark-submit \
        "/opt/spark/jobs/${job#spark/jobs/}"

    if [ $? -ne 0 ]; then
        echo "❌ FAILED: $job"
        exit 1
    fi

    echo "✅ DONE: $job"
done

echo "========================================"
echo "🎉 ALL ANALYTICS JOBS COMPLETED"
echo "========================================"
