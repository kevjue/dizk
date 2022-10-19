<h1 align="center">DIZK/CIRCOM Integration</h1>

As part of the 0xPARC 2022 summer residency, this dizk-fork was created to enable DIZK to generate groth16 proofs for circom circuits.  Note that this code is VERY experimental.

## Build instructions

1) Install dependencies listed [__here__](https://github.com/kevjue/dizk/blob/kevjue/circom_integration/README.md#build-guide)
2) Install spark python cluster manager [__Flinkrock__](https://pypi.org/project/Flintrock/)
3) Compile using the command "**mvn package -DskipTests**" within the dizk root directory.


## Run instructions
Set your AWS env variables
```
export AWS_ACCESS_KEY_ID=<Your AWS access key>
export AWS_SECRET_ACCESS_KEY=<Your AWS secret key>
export AWS_EC2_VPC_ID=<Your AWS VPC ID>
export AWS_EC2_IDENTITY_FILE=<Your ssh cer/pem file>
export AWS_EC2_KEY_NAME=<Your EC2 keypair name>
export AWS_EC2_SUBNET_ID=<Your EC2 subnet id>
```


Spin up the AWS spark cluster and install Spark
```$xslt
python3 -m flintrock launch --num-slaves 8 --spark-version 2.2.0 --hdfs-version=2.6 --ec2-key-name=$AWS_EC2_KEY_NAME --ec2-identity-file=$AWS_EC2_IDENTITY_FILE --ec2-ami=ami-0aeeebd8d2ab47354 --ec2-user=ec2-user --spark-download-source=https://archive.apache.org/dist/spark/spark-2.2.0/ --ec2-vpc-id=$AWS_EC2_VPC_ID --ec2-subnet-id=$AWS_EC2_SUBNET_ID --ec2-instance-type=t3.2xlarge --ec2-security-group spark-dashboard --ec2-min-root-ebs-size-gb 400 --spark-executor-instances 50 test-cluster
```

Copy over the your DIZK jar file to the cluster
```xslt
python3 -m flintrock copy-file --ec2-identity-file=$AWS_EC2_IDENTITY_FILE --ec2-vpc-id=$AWS_EC2_VPC_ID --ec2-user=ec2-user test-cluster <path to compiled dizk jar (e.g. ~/dizk/target/dizk-1.0.jar>> /home/ec2-user/dizk-1.0.jar
```

Install s3cmd on cluster machines (to download needed files for sample DIZK run)
```xslt
python3 -m flintrock run-command --ec2-identity-file=$AWS_EC2_IDENTITY_FILE --ec2-vpc-id=$AWS_EC2_VPC_ID --ec2-user=ec2-user test-cluster "wget https://sourceforge.net/projects/s3tools/files/s3cmd/2.2.0/s3cmd-2.2.0.tar.gz; tar xzf s3cmd-2.2.0.tar.gz; cd s3cmd-2.2.0; sudo python setup.py install"
```

Locally create a .s3cfg file using this [__template__](https://s3tools.org/kb/item14.htm).

 Copy over your s3cfg file to the cluster machines 
```xslt
python3 -m flintrock copy-file --ec2-identity-file=$AWS_EC2_IDENTITY_FILE --ec2-vpc-id=$AWS_EC2_VPC_ID --ec2-user=ec2-user test-cluster <Path to your s3cfg file> /home/ec2-user/.s3cfg
```

Download and unzip the groth16 proving key
```xslt
python3 -m flintrock run-command --ec2-identity-file=$AWS_EC2_IDENTITY_FILE --ec2-vpc-id=$AWS_EC2_VPC_ID --ec2-user=ec2-user test-cluster "s3cmd get s3://devdos/test_ecdsa_verify.pk.tar.gz ."

python3 -m flintrock run-command --ec2-identity-file=$AWS_EC2_IDENTITY_FILE --ec2-vpc-id=$AWS_EC2_VPC_ID --ec2-user=ec2-user test-cluster tar xvfz /home/ec2-user/test_ecdsa_verify.pk.tar.gz
```

Download witness file, groth16 verification key, public input file from s3
```xslt
python3 -m flintrock run-command --ec2-identity-file=$AWS_EC2_IDENTITY_FILE --ec2-vpc-id=$AWS_EC2_VPC_ID --ec2-user=ec2-user test-cluster "s3cmd get s3://dizk-circom-demo/test_ecdsa_verify.wtns.json ."

python3 -m flintrock run-command --ec2-identity-file=$AWS_EC2_IDENTITY_FILE --ec2-vpc-id=$AWS_EC2_VPC_ID --ec2-user=ec2-user test-cluster "s3cmd get s3://dizk-circom-demo/test_ecdsa_verify.vk ."

python3 -m flintrock run-command --ec2-identity-file=$AWS_EC2_IDENTITY_FILE --ec2-vpc-id=$AWS_EC2_VPC_ID --ec2-user=ec2-user test-cluster "s3cmd get s3://dizk-circom-demo/test_ecdsa_verify.input.json ."
```

Make events directory
```xslt
python3 -m flintrock run-command --ec2-identity-file=$AWS_EC2_IDENTITY_FILE --ec2-vpc-id=$AWS_EC2_VPC_ID  --ec2-user=ec2-user test-cluster "mkdir /tmp/spark-events"
```

SSH onto the cluster's master node
```xlst
python3 -m flintrock login --ec2-identity-file=$AWS_EC2_IDENTITY_FILE --ec2-user=ec2-user --ec2-vpc-id=$AWS_EC2_VPC_ID test-cluster
```

On the master node, run proof generation job
```xlst
./spark/bin/spark-submit --conf spark.driver.memory=3g --conf spark.executor.memory=3g --conf spark.executor.cores=1 --conf spark.executor.instances=50 --conf spark.logConf=true --conf spark.memory.fraction=0.95 --conf spark.memory.storageFraction=0.3 --conf spark.eventLog.enabled=true --total-executor-cores 50 --deploy-mode=client --class=profiler.Profiler --master=spark://<computer name of master node>:7077 dizk-1.0.jar circom_generate_proof /home/ec2-user/test_ecdsa_verify.pk /home/ec2-user/test_ecdsa_verify.wtns.json /home/ec2-user/test_ecdsa_verify.proof distributed
```

The cluster dashboard (url: https://\<computer name of master node>:8080) can be loaded to view the running job.

Print out the generated proof
```
./spark/bin/spark-submit --deploy-mode=client --class=profiler.Profiler  --master=spark://localhost:7077 dizk-1.0.jar print_proof /home/ec2-user/test_ecdsa_verify.proof distributed
```

Verify the proof
```
./spark/bin/spark-submit --deploy-mode=client --class=profiler.Profiler  --master=spark://localhost:7077 dizk-1.0.jar circom_verify_proof  /home/ec2-user/test_ecdsa_verify.vk /home/ec2-user/test_ecdsa_verify.proof /home/ec2-user/test_ecdsa_verify.input.json
```

Destroy the cluster
```
python3 -m flintrock destroy --ec2-vpc-id=$AWS_EC2_VPC_ID test-cluster
```


