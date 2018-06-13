spark-submit \
--class Ch06.StatefulToKafka \
--master spark://T460p:7077  \
--name "ShopOrderTracking-production" \
--deploy-mode cluster \
--driver-memory 2g \
--executor-memory 2g \
--executor-cores 2 \
--conf "spark.executor.instances=1" \
--conf "spark.streaming.backpressure.initialRate=100" \
--conf "spark.streaming.backpressure.enabled=true" \
--conf "spark.streaming.kafka.maxRatePerPartition=2000" \
spark-in-action-assembly-0.1.jar