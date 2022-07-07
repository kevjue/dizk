package profiler;

import configuration.Configuration;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import algebra.curves.barreto_naehrig.bn254a.BN254aFields.BN254aFr;
import algebra.curves.barreto_naehrig.bn254a.BN254aG1;
import algebra.curves.barreto_naehrig.bn254a.BN254aG2;
import algebra.curves.barreto_naehrig.bn254a.BN254aGT;
import algebra.curves.barreto_naehrig.bn254a.BN254aPairing;
import algebra.curves.barreto_naehrig.bn254a.bn254a_parameters.BN254aG1Parameters;
import algebra.curves.barreto_naehrig.bn254a.bn254a_parameters.BN254aG2Parameters;
import profiler.profiling.*;
import profiler.utils.SparkUtils;
import relations.objects.Assignment;
import relations.r1cs.R1CSRelation;
import relations.r1cs.R1CSRelationRDD;
import scala.Tuple2;
import utils.Circom;
import utils.Serialize;
import zk_proof_systems.zkSNARK.DistributedProver;
import zk_proof_systems.zkSNARK.DistributedSetup;
import zk_proof_systems.zkSNARK.SerialProver;
import zk_proof_systems.zkSNARK.SerialSetup;
import zk_proof_systems.zkSNARK.Verifier;
import zk_proof_systems.zkSNARK.objects.CRS;
import zk_proof_systems.zkSNARK.objects.Proof;
import zk_proof_systems.zkSNARK.objects.ProvingKey;
import zk_proof_systems.zkSNARK.objects.ProvingKeyRDD;
import zk_proof_systems.zkSNARK.objects.VerificationKey;

public class Profiler {

    public static void serialApp(final String app, final Configuration config, final long size) {
        System.out.format("\n[Profiler] - Start Serial %s - %d size\n", SparkUtils.appName(app), size);

        if (app.equals("fft")) {
            FFTProfiling.serialFFTProfiling(config, size);
        } else if (app.equals("lagrange")) {
            LagrangeProfiling.serialLagrangeProfiling(config, size);
        } else if (app.equals("fmsm-g1")) {
            FixedBaseMSMProfiling.serialFixedBaseMSMG1Profiling(config, size);
        } else if (app.equals("fmsm-g2")) {
            FixedBaseMSMProfiling.serialFixedBaseMSMG2Profiling(config, size);
        } else if (app.equals("vmsm-g1")) {
            VariableBaseMSMProfiling.serialVariableBaseMSMG1Profiling(config, size);
        } else if (app.equals("vmsm-g2")) {
            VariableBaseMSMProfiling.serialVariableBaseMSMG2Profiling(config, size);
        } else if (app.equals("relation")) {
            R1CStoQAPRelationProfiling.serialQAPRelation(config, size);
        } else if (app.equals("witness")) {
            R1CStoQAPWitnessProfiling.serialQAPWitness(config, size);
        } else if (app.equals("zksnark")) {
            ZKSNARKProfiling.serialzkSNARKProfiling(config, size);
        } else if (app.equals("zksnark-large")) {
            ZKSNARKProfiling.serialzkSNARKLargeProfiling(config, size);
        }

        System.out.format("\n[Profiler] - End Serial %s - %d size\n", SparkUtils.appName(app), size);
    }

    public static void distributedApp(final String app, final Configuration config, final long size) {
        System.out.format(
                "\n[Profiler] - Start Distributed %s - %d executors - %d partitions - %d size\n\n",
                SparkUtils.appName(app),
                config.numExecutors(),
                config.numPartitions(),
                size);

        if (app.equals("fft")) {
            FFTProfiling.distributedFFTProfiling(config, size);
        } else if (app.equals("lagrange")) {
            LagrangeProfiling.distributedLagrangeProfiling(config, size);
        } else if (app.equals("fmsm-g1")) {
            FixedBaseMSMProfiling.distributedFixedBaseMSMG1Profiling(config, size);
        } else if (app.equals("fmsm-g2")) {
            FixedBaseMSMProfiling.distributedFixedBaseMSMG2Profiling(config, size);
        } else if (app.equals("vmsm-g1")) {
            VariableBaseMSMProfiling.distributedVariableBaseMSMG1Profiling(config, size);
        } else if (app.equals("vmsm-g2")) {
            VariableBaseMSMProfiling.distributedVariableBaseMSMG2Profiling(config, size);
        } else if (app.equals("relation")) {
            R1CStoQAPRelationProfiling.distributedQAPRelation(config, size);
        } else if (app.equals("witness")) {
            R1CStoQAPWitnessProfiling.distributedQAPWitness(config, size);
        } else if (app.equals("zksnark")) {
            ZKSNARKProfiling.distributedzkSNARKProfiling(config, size);
        } else if (app.equals("zksnark-large")) {
            ZKSNARKProfiling.distributedzkSNARKLargeProfiling(config, size);
        } else if (app.equals("vmsm-sorted-g1")) {
            VariableBaseMSMProfiling.distributedSortedVariableBaseMSMG1Profiling(config, size);
        }

        System.out.format(
                "\n[Profiler] - End Distributed %s - %d executors - %d partitions - %d size\n\n",
                SparkUtils.appName(app),
                config.numExecutors(),
                config.numPartitions(),
                size);
    }

    public static void matmulTest(final Configuration config,
                                  int n1, int n2, int n3, int b1, int b2, int b3,
                                  String app) {
        if (app.equals("matmul")) {
            MatrixMultiplicationProfiling.MatrixMultiplicationProfile(config, n1, n2, n3, b1, b2, b3);
        } else {
            MatrixMultiplicationProfiling.MatrixMultiplicationProfile(config, n1, n2, n3, b1, b2, b3);
        }
    }

    public static void lrTest(final Configuration config,
                              int n, int d, int bn, int bd, String app) {
        if (app.equals("regression")) {
            MatrixMultiplicationProfiling.LRProfile(config, n, d, bn, bd);
        }
    }

    public static void matrixMultiplicationTest(
            final Configuration config, int n1, int n2, int n3, int b1, int b2, int b3) {
        MatrixMultiplicationProfiling.MatrixMultiplicationProfile(config, n1, n2, n3, b1, b2, b3);
    }

    public static void lrTest(
            final Configuration config, int n, int d, int bn, int bd) {
        MatrixMultiplicationProfiling.LRProfile(config, n, d, bn, bd);
    }

    public static void gaussianTest(
            final Configuration config, int n, int d, int bn, int bd) {
        MatrixMultiplicationProfiling.GaussianProfile(config, n, d, bn, bd);
    }

    public static void main(String[] args) throws IOException {
        if (args.length > 0) {
            String input = args[0].toLowerCase();
            if (input.equals("matmul") || input.equals("matmul_full")) {
                final String app = args[0].toLowerCase();
                final int numExecutors = Integer.parseInt(args[1]);
                final int numCores = Integer.parseInt(args[2]);
                final int numMemory = Integer.parseInt(args[3].substring(0, args[3].length() - 1));
                final int numPartitions = Integer.parseInt(args[4]);

                final SparkSession spark = SparkSession.builder().appName(SparkUtils.appName(app))
                        .getOrCreate();
                spark.sparkContext().conf().set("spark.files.overwrite", "true");
                spark.sparkContext().conf()
                        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
                spark.sparkContext().conf().registerKryoClasses(SparkUtils.zksparkClasses());

                JavaSparkContext sc;
                sc = new JavaSparkContext(spark.sparkContext());
                final Configuration config = new Configuration(numExecutors,
                        numCores,
                        numMemory,
                        numPartitions,
                        sc,
                        StorageLevel.MEMORY_AND_DISK_SER());

                final int n1 = Integer.parseInt(args[5]);
                final int n2 = Integer.parseInt(args[6]);
                final int n3 = Integer.parseInt(args[7]);
                final int b1 = Integer.parseInt(args[8]);
                final int b2 = Integer.parseInt(args[9]);
                final int b3 = Integer.parseInt(args[10]);

                matmulTest(config, n1, n2, n3, b1, b2, b3, input);
            } else if (input.equals("regression")) {
                final String app = args[0].toLowerCase();
                final int numExecutors = Integer.parseInt(args[1]);
                final int numCores = Integer.parseInt(args[2]);
                final int numMemory = Integer.parseInt(args[3].substring(0, args[3].length() - 1));
                final int numPartitions = Integer.parseInt(args[4]);

                final SparkSession spark = SparkSession.builder().appName(SparkUtils.appName(app))
                        .getOrCreate();
                spark.sparkContext().conf().set("spark.files.overwrite", "true");
                spark.sparkContext().conf()
                        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
                spark.sparkContext().conf().registerKryoClasses(SparkUtils.zksparkClasses());

                JavaSparkContext sc;
                sc = new JavaSparkContext(spark.sparkContext());
                final Configuration config = new Configuration(numExecutors,
                        numCores,
                        numMemory,
                        numPartitions,
                        sc,
                        StorageLevel.MEMORY_AND_DISK_SER());

                final int n = Integer.parseInt(args[5]);
                final int d = Integer.parseInt(args[6]);
                final int bn = Integer.parseInt(args[7]);
                final int bd = Integer.parseInt(args[8]);

                lrTest(config, n, d, bn, bd, input);

            } else if (input.equals("gaussian")) {
                final String app = args[0].toLowerCase();
                final int numExecutors = Integer.parseInt(args[1]);
                final int numCores = Integer.parseInt(args[2]);
                final int numMemory = Integer.parseInt(args[3].substring(0, args[3].length() - 1));
                final int numPartitions = Integer.parseInt(args[4]);

                final SparkSession spark = SparkSession.builder().appName(SparkUtils.appName(app))
                        .getOrCreate();
                spark.sparkContext().conf().set("spark.files.overwrite", "true");
                spark.sparkContext().conf()
                        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
                spark.sparkContext().conf().registerKryoClasses(SparkUtils.zksparkClasses());

                JavaSparkContext sc;
                sc = new JavaSparkContext(spark.sparkContext());
                final Configuration config = new Configuration(numExecutors,
                        numCores,
                        numMemory,
                        numPartitions,
                        sc,
                        StorageLevel.MEMORY_AND_DISK_SER());

                final int n = Integer.parseInt(args[5]);
                final int d = Integer.parseInt(args[6]);
                final int bn = Integer.parseInt(args[7]);
                final int bd = Integer.parseInt(args[8]);

                gaussianTest(config, n, d, bn, bd);
            } else if (input.equals("circom_setup")) {
                // TODO: Modify this subcommand to accept a pot file.
                final String r1csFilename = args[1];
                final String provingKeyFilename = args[2];
                final String verificationKeyFilename = args[3];
                final String runMode = args[4];  // Should be "serial" or "distributed"

                FileReader r1csFR = null;
                try {
                    r1csFR = new FileReader(r1csFilename);
                } catch (FileNotFoundException  e) {
                    System.out.println("File not found error: " + r1csFilename);
                    System.exit(1);
                }

                R1CSRelation<BN254aFr> r1cs = Circom.readR1CSFile(r1csFR);

                final BN254aFr fieldFactory = new BN254aFr(2L);
                final BN254aG1 g1Factory = new BN254aG1Parameters().ONE();
                final BN254aG2 g2Factory = new BN254aG2Parameters().ONE();
                final BN254aPairing pairing = new BN254aPairing();

                CRS<BN254aFr, BN254aG1, BN254aG2, BN254aGT> crs;
                if (runMode.equals("serial")) {
                    final Configuration config = new Configuration();
                    config.setDebugFlag(true);
                    config.setSeed((long) 57345);
                    crs = SerialSetup.generate(r1cs, fieldFactory, g1Factory, g2Factory, pairing, config);
                } else {
                    assert(runMode.equals("distributed"));

                    final int numExecutors = 1;
                    final int numCores = 1;
                    final int numMemory = 8;
                    final int numPartitions = SparkUtils.numPartitions(numExecutors, r1cs.numConstraints());

                    final SparkConf conf = new SparkConf().setMaster("local").setAppName("default");
                    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
                    //conf.set("spark.kryo.registrationRequired", "true");
                    //conf.registerKryoClasses(SparkUtils.zksparkClasses());

                    JavaSparkContext sc;
                    sc = new JavaSparkContext(conf);

                    final Configuration config = new Configuration(
                            numExecutors,
                            numCores,
                            numMemory,
                            numPartitions,
                            sc,
                            StorageLevel.MEMORY_AND_DISK_SER());
                    config.setDebugFlag(true);
                    config.setSeed((long) 57345);
                    final R1CSRelationRDD<BN254aFr> distributedR1CS = new R1CSRelationRDD<BN254aFr>(r1cs, config);
                    crs = DistributedSetup.generate(distributedR1CS, fieldFactory, g1Factory, g2Factory, pairing, config);
                }

                if (runMode.equals("serial")) {
                    Serialize.SerializeObject(crs.provingKey(), provingKeyFilename);
                } else {  // "runMode == distributed"
                    crs.provingKeyRDD().saveAsObjectFile(provingKeyFilename);
                }

                Serialize.SerializeObject(crs.verificationKey(), verificationKeyFilename);
            } else if (input.equals("circom_generate_proof")) {
                final String provingKeyFilename = args[1];
                final String witnessFilename = args[2];
                final String proofFilename = args[3];
                final String runMode = args[4];  // Should be "serial" or "distributed"

                final Configuration config;
                ProvingKey<BN254aFr, BN254aG1, BN254aG2> pk = null;
                ProvingKeyRDD<BN254aFr, BN254aG1, BN254aG2> pkRDD = null;
                int numInputs;
                if (runMode.equals("serial")) {
                    config = new Configuration();
                    config.setDebugFlag(true);
                    pk = (ProvingKey<BN254aFr, BN254aG1, BN254aG2>) Serialize.UnserializeObject(provingKeyFilename);
                    numInputs = pk.r1cs().numInputs();
                } else {  // "runMode == distributed"
                    final int numExecutors = 1;
                    final int numCores = 1;
                    final int numMemory = 8;

                    final long size = ProvingKeyRDD.getNumConstraintsFromFile(provingKeyFilename);
                    final int numPartitions = SparkUtils.numPartitions(numExecutors, size);

                    final SparkConf conf = new SparkConf().setMaster("local").setAppName("default");
                    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
                    //conf.set("spark.kryo.registrationRequired", "true");
                    //conf.registerKryoClasses(SparkUtils.zksparkClasses());

                    JavaSparkContext sc;
                    sc = new JavaSparkContext(conf);

                    config = new Configuration(
                        numExecutors,
                        numCores,
                        numMemory,
                        numPartitions,
                        sc,
                        StorageLevel.MEMORY_AND_DISK_SER());
                    config.setDebugFlag(true);

                    pkRDD = ProvingKeyRDD.loadFromObjectFile(provingKeyFilename, config);
                    numInputs = pkRDD.r1cs().numInputs();
                }

                FileReader witnessFR = null;
                try {
                    witnessFR = new FileReader(witnessFilename);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }

                final Tuple2<Assignment<BN254aFr>, Assignment<BN254aFr>> witnessTuple = Circom.readWitnessFile(witnessFR, numInputs);
                final BN254aFr fieldFactory = new BN254aFr(2L);
                config.setSeed((long) 646373);
                Proof<BN254aG1, BN254aG2> proof;
                if (runMode.equals("serial")) {
                    proof = SerialProver.prove(pk, witnessTuple._1(), witnessTuple._2(), fieldFactory, config);
                } else {  // "runMode == distributed"
                    List<BN254aFr> witness = witnessTuple._2().elements();
                    List<Tuple2<Long, BN254aFr>> witnessPairs = IntStream.range(0, witness.size()).mapToObj(i -> new Tuple2<>(new Long(i), witness.get(i))).collect(Collectors.toList());
                    JavaPairRDD<Long, BN254aFr> witnessRDD = config.sparkContext().parallelizePairs(witnessPairs);
                    proof = DistributedProver.prove(pkRDD, witnessTuple._1(), witnessRDD, fieldFactory, config);
                }

                Serialize.SerializeObject(proof, proofFilename);

            } else if (input.equals("circom_verify_proof")) {
                final String verificationKeyFilename = args[1];
                final String proofFilename = args[2];
                final String publicInputFilename = args[3];

                VerificationKey<BN254aG1, BN254aG2, BN254aGT> vk = (VerificationKey<BN254aG1, BN254aG2, BN254aGT>) Serialize.UnserializeObject(verificationKeyFilename);
                Proof<BN254aG1, BN254aG2> proof = (Proof<BN254aG1, BN254aG2>) Serialize.UnserializeObject(proofFilename);

                FileReader publicInputFR = null;
                try {
                    publicInputFR = new FileReader(publicInputFilename);
                } catch (FileNotFoundException  e) {
                    System.out.println("File not found error: " + publicInputFilename);
                    System.exit(1);
                }

                Assignment<BN254aFr> publicInput = Circom.readPublicInputFile(publicInputFR);

                final BN254aPairing pairing = new BN254aPairing();
                final Configuration config = new Configuration();
                boolean isValid = Verifier.verify(vk, publicInput, proof, pairing, config);

                System.out.println("Proof validity: " + isValid);

            } else if (input.equals("print_keys")) {
                final String provingKeyFilename = args[1];
                final String verificationKeyFilename = args[2];
                final String runMode = args[3];

                Configuration config = null;
                ProvingKey<BN254aFr, BN254aG1, BN254aG2> pk = null;
                ProvingKeyRDD<BN254aFr, BN254aG1, BN254aG2> pkRDD = null;
                BN254aG1 alphaG1 = null;
                BN254aG1 betaG1 = null;
                BN254aG2 betaG2 = null;
                BN254aG1 deltaG1 = null;
                BN254aG2 deltaG2 = null;
                List<BN254aG1> deltaABCG1 = null;
                List<BN254aG1> queryA = null;
                List<Tuple2<BN254aG1, BN254aG2>> queryB = null;
                List<BN254aG1> queryH = null;
                if (runMode.equals("serial")) {
                    config = new Configuration();
                    config.setDebugFlag(true);
                    pk = (ProvingKey<BN254aFr, BN254aG1, BN254aG2>) Serialize.UnserializeObject(provingKeyFilename);

                    alphaG1 = pk.alphaG1();
                    betaG1 = pk.betaG1();
                    betaG2 = pk.betaG2();
                    deltaG1 = pk.deltaG1();
                    deltaG2 = pk.deltaG2();
                    deltaABCG1 = pk.deltaABCG1();
                    queryA = pk.queryA();
                    queryB = pk.queryB();
                    queryH = pk.queryH();

                } else {  // "runMode == distributed"
                    final int numExecutors = 1;
                    final int numCores = 1;
                    final int numMemory = 8;

                    final long size = ProvingKeyRDD.getNumConstraintsFromFile(provingKeyFilename);
                    final int numPartitions = SparkUtils.numPartitions(numExecutors, size);

                    final SparkConf conf = new SparkConf().setMaster("local").setAppName("default");
                    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
                    //conf.set("spark.kryo.registrationRequired", "true");
                    //conf.registerKryoClasses(SparkUtils.zksparkClasses());

                    JavaSparkContext sc;
                    sc = new JavaSparkContext(conf);

                    config = new Configuration(
                        numExecutors,
                        numCores,
                        numMemory,
                        numPartitions,
                        sc,
                        StorageLevel.MEMORY_AND_DISK_SER());
                    config.setDebugFlag(true);

                    pkRDD = ProvingKeyRDD.loadFromObjectFile(provingKeyFilename, config);

                    alphaG1 = pkRDD.alphaG1();
                    betaG1 = pkRDD.betaG1();
                    betaG2 = pkRDD.betaG2();
                    deltaG1 = pkRDD.deltaG1();
                    deltaG2 = pkRDD.deltaG2();
                    deltaABCG1 = pkRDD.deltaABCG1().sortByKey().collect().stream().map(e -> e._2()).collect(Collectors.toList());
                    queryA = pkRDD.queryA().sortByKey().collect().stream().map(e -> e._2()).collect(Collectors.toList());
                    queryB = pkRDD.queryB().sortByKey().collect().stream().map(e -> e._2()).collect(Collectors.toList());
                    queryH = pkRDD.queryH().sortByKey().collect().stream().map(e -> e._2()).collect(Collectors.toList());
                }

                System.out.println("Proving key");
                System.out.println("\talphaG1:\t" + alphaG1.toAffineCoordinates());
                System.out.println("\tbetaG1:\t\t" + betaG1.toAffineCoordinates());
                System.out.println("\tbetaG2:\t\t" + betaG2.toAffineCoordinates());
                System.out.println("\tdeltaG1:\t" + deltaG1.toAffineCoordinates());
                System.out.println("\tdeltaG2:\t" + deltaG2.toAffineCoordinates());

                System.out.println("\tdeltaABCG1:\n\t[");
                for (int i = 0; i < deltaABCG1.size(); i++) {
                    System.out.println("\t\t" + deltaABCG1.get(i).toAffineCoordinates());
                }
                System.out.println("\t]");

                System.out.println("\tqueryA:\n\t[");
                for (int i = 0; i < queryA.size(); i++) {
                    System.out.println("\t\t" + queryA.get(i).toAffineCoordinates());
                }
                System.out.println("\t]");

                System.out.println("\tqueryB G1:\n\t[");
                for (int i = 0; i < queryB.size(); i++) {
                    System.out.println("\t\t" + queryB.get(i)._1().toAffineCoordinates());
                }
                System.out.println("\t]");


                System.out.println("\tqueryB G2:\n\t[");
                for (int i = 0; i < queryB.size(); i++) {
                    System.out.println("\t\t" + queryB.get(i)._2().toAffineCoordinates());
                }
                System.out.println("\t]");

                System.out.println("\tqueryH:\n\t[");
                for (int i = 0; i < queryH.size(); i++) {
                    System.out.println("\t\t" + queryH.get(i).toAffineCoordinates());
                }
                System.out.println("\t]");


                VerificationKey<BN254aG1, BN254aG2, BN254aGT> vk = (VerificationKey<BN254aG1, BN254aG2, BN254aGT>) Serialize.UnserializeObject(verificationKeyFilename);
                System.out.println("\nVerification key");
                System.out.println("\talphaG1BetaG2:\t" + vk.alphaG1betaG2());
                System.out.println("\tgammaG2:\t" + vk.gammaG2().toAffineCoordinates());
                System.out.println("\tdeltaG1:\t" + vk.deltaG2());

                System.out.println("\tgammaABC:\n\t[");
                for (int i = 0; i < vk.gammaABC().size(); i++) {
                    System.out.println("\t\t" + vk.gammaABC().get(i).toAffineCoordinates());
                }
                System.out.println("\t]");
            } else if (input.equals("print_proof")) {
                final String proofFilename = args[1];

                Proof<BN254aG1, BN254aG2> proof = (Proof<BN254aG1, BN254aG2>) Serialize.UnserializeObject(proofFilename);

                System.out.println("Proof");
                System.out.println("\tgA: \t" + proof.gA().toAffineCoordinates());
                System.out.println("\tgB: \t" + proof.gB().toAffineCoordinates());
                System.out.println("\tgC: \t" + proof.gC().toAffineCoordinates());

            } else if (args.length == 2) {
                final String app = args[0].toLowerCase();
                final long size = (long) Math.pow(2, Long.parseLong(args[1]));

                final Configuration config = new Configuration();
                serialApp(app, config, size);
            } else if (args.length == 5 || args.length == 6) {
                final int numExecutors = Integer.parseInt(args[0]);
                final int numCores = Integer.parseInt(args[1]);
                final int numMemory = Integer.parseInt(args[2].substring(0, args[2].length() - 1));
                final String app = args[3].toLowerCase();
                final long size = (long) Math.pow(2, Long.parseLong(args[4]));
                int numPartitions;
                if (args.length == 5) {
                    numPartitions = SparkUtils.numPartitions(numExecutors, size);
                } else {
                    numPartitions = Integer.parseInt(args[5]);
                }

                final SparkSession spark = SparkSession.builder().appName(SparkUtils.appName(app))
                        .getOrCreate();
                spark.sparkContext().conf().set("spark.files.overwrite", "true");
                spark.sparkContext().conf()
                        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
                spark.sparkContext().conf().registerKryoClasses(SparkUtils.zksparkClasses());

                JavaSparkContext sc;
                sc = new JavaSparkContext(spark.sparkContext());

                final Configuration config = new Configuration(
                        numExecutors,
                        numCores,
                        numMemory,
                        numPartitions,
                        sc,
                        StorageLevel.MEMORY_AND_DISK_SER());

                distributedApp(app, config, size);
            } else {
                System.out.println(
                        "Args: {numExecutors} {numCores} {numMemory} {app} {size (log2)} {numPartitions(opt)}");
            }
        } else {
            final String app = "zksnark-large";
            final int numExecutors = 1;
            final int numCores = 1;
            final int numMemory = 8;
            final int size = 1024;

            final int numPartitions = SparkUtils.numPartitions(numExecutors, size);

            final SparkConf conf = new SparkConf().setMaster("local").setAppName("default");
            conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            conf.set("spark.kryo.registrationRequired", "true");
            conf.registerKryoClasses(SparkUtils.zksparkClasses());

            JavaSparkContext sc;
            sc = new JavaSparkContext(conf);

            final Configuration config = new Configuration(
                    numExecutors,
                    numCores,
                    numMemory,
                    numPartitions,
                    sc,
                    StorageLevel.MEMORY_AND_DISK_SER());
            distributedApp(app, config, size);
        }
    }

}
