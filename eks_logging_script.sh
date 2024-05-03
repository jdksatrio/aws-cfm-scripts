#!/bin/sh
for i in $(aws eks list-clusters --query 'clusters[]' --output text)
do
  echo $i `aws eks describe-cluster --name $i --query 'cluster.logging.clusterLogging[].enabled' --output text`
done
