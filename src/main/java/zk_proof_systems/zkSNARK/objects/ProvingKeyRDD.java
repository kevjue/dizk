/* @file
 *****************************************************************************
 * @author     This file is part of zkspark, developed by SCIPR Lab
 *             and contributors (see AUTHORS).
 * @copyright  MIT license (see LICENSE file)
 *****************************************************************************/

package zk_proof_systems.zkSNARK.objects;

import algebra.fields.AbstractFieldElementExpanded;
import configuration.Configuration;
import algebra.curves.AbstractG1;
import algebra.curves.AbstractG2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import relations.r1cs.R1CSRelationRDD;
import scala.Tuple2;
import utils.Serialize;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;

public class ProvingKeyRDD<FieldT extends AbstractFieldElementExpanded<FieldT>, G1T extends
        AbstractG1<G1T>, G2T extends AbstractG2<G2T>> implements
        Serializable {

    private final G1T alphaG1;
    private final G1T betaG1;
    private final G2T betaG2;
    private final G1T deltaG1;
    private final G2T deltaG2;
    private final JavaPairRDD<Long, G1T> deltaABCG1;
    private final JavaPairRDD<Long, G1T> queryA;
    private final JavaPairRDD<Long, Tuple2<G1T, G2T>> queryB;
    private final JavaPairRDD<Long, G1T> queryH;
    private final R1CSRelationRDD<FieldT> r1cs;

    public ProvingKeyRDD(
            final G1T _alphaG1,
            final G1T _betaG1,
            final G2T _betaG2,
            final G1T _deltaG1,
            final G2T _deltaG2,
            final JavaPairRDD<Long, G1T> _deltaABCG1,
            final JavaPairRDD<Long, G1T> _queryA,
            final JavaPairRDD<Long, Tuple2<G1T, G2T>> _queryB,
            final JavaPairRDD<Long, G1T> _queryH,
            final R1CSRelationRDD<FieldT> _r1cs) {
        alphaG1 = _alphaG1;
        betaG1 = _betaG1;
        betaG2 = _betaG2;
        deltaG1 = _deltaG1;
        deltaG2 = _deltaG2;
        deltaABCG1 = _deltaABCG1;
        queryA = _queryA;
        queryB = _queryB;
        queryH = _queryH;
        r1cs = _r1cs;
    }

    public G1T alphaG1() {
        return alphaG1;
    }

    public G1T betaG1() {
        return betaG1;
    }

    public G2T betaG2() {
        return betaG2;
    }

    public G1T deltaG1() {
        return deltaG1;
    }

    public G2T deltaG2() {
        return deltaG2;
    }

    public JavaPairRDD<Long, G1T> deltaABCG1() {
        return deltaABCG1;
    }

    public JavaPairRDD<Long, G1T> queryA() {
        return queryA;
    }

    public JavaPairRDD<Long, Tuple2<G1T, G2T>> queryB() {
        return queryB;
    }

    public JavaPairRDD<Long, G1T> queryH() {
        return queryH;
    }

    public R1CSRelationRDD<FieldT> r1cs() {
        return r1cs;
    }

    public void saveAsObjectFile(String dirName) throws IOException {
        File directory = new File(dirName);

        directory.mkdir();

        Serialize.SerializeObject(alphaG1, Paths.get(dirName, "alphaG1").toString());
        Serialize.SerializeObject(betaG1, Paths.get(dirName, "betaG1").toString());
        Serialize.SerializeObject(betaG2, Paths.get(dirName, "betaG2").toString());
        Serialize.SerializeObject(deltaG1, Paths.get(dirName, "deltaG1").toString());
        Serialize.SerializeObject(deltaG2, Paths.get(dirName, "deltaG2").toString());

        deltaABCG1.saveAsObjectFile(Paths.get(dirName, "deltaABC").toString());
        queryA.saveAsObjectFile(Paths.get(dirName, "queryA").toString());
        queryB.saveAsObjectFile(Paths.get(dirName, "queryB").toString());
        queryH.saveAsObjectFile(Paths.get(dirName, "queryH").toString());
        r1cs.saveAsObjectFile(Paths.get(dirName, "r1cs").toString());
    }

    public static <FieldT extends AbstractFieldElementExpanded<FieldT>, G1T extends
    AbstractG1<G1T>, G2T extends AbstractG2<G2T>> ProvingKeyRDD<FieldT, G1T, G2T> loadFromObjectFile(String dirName, Configuration config) throws IOException{
        final G1T _alphaG1 = (G1T) Serialize.UnserializeObject(Paths.get(dirName, "alphaG1").toString());
        final G1T _betaG1 = (G1T) Serialize.UnserializeObject(Paths.get(dirName, "betaG1").toString());
        final G2T _betaG2 = (G2T) Serialize.UnserializeObject(Paths.get(dirName, "betaG2").toString());
        final G1T _deltaG1 = (G1T) Serialize.UnserializeObject(Paths.get(dirName, "deltaG1").toString());
        final G2T _deltaG2 = (G2T) Serialize.UnserializeObject(Paths.get(dirName, "deltaG2").toString());

        JavaRDD<Tuple2<Long, G1T>> _deltaABCRDD = config.sparkContext().objectFile(Paths.get(dirName, "deltaABC").toString());
        JavaPairRDD<Long, G1T> _deltaABC =  _deltaABCRDD.mapToPair(e -> e);

        JavaRDD<Tuple2<Long, G1T>> _queryARDD = config.sparkContext().objectFile(Paths.get(dirName, "queryA").toString());
        JavaPairRDD<Long, G1T> _queryA = _queryARDD.mapToPair(e -> e);

        JavaRDD<Tuple2<Long, Tuple2<G1T, G2T>>> _queryBRDD = config.sparkContext().objectFile(Paths.get(dirName, "queryB").toString());
        JavaPairRDD<Long, Tuple2<G1T, G2T>> _queryB =  _queryBRDD.mapToPair(e -> e);

        JavaRDD<Tuple2<Long, G1T>> _queryHRDD = config.sparkContext().objectFile(Paths.get(dirName, "queryH").toString());
        JavaPairRDD<Long, G1T> _queryH =  _queryHRDD.mapToPair(e -> e);

        R1CSRelationRDD<FieldT> _r1cs = R1CSRelationRDD.loadFromObjectFile(Paths.get(dirName, "r1cs").toString(), config);

        return new ProvingKeyRDD<FieldT, G1T, G2T>(_alphaG1,
                                                _betaG1,
                                                _betaG2,
                                                _deltaG1,
                                                _deltaG2,
                                                _deltaABC,
                                                _queryA,
                                                _queryB,
                                                _queryH,
                                                _r1cs);
    }

    public static long getNumConstraintsFromFile(String dirName) throws IOException {
        return R1CSRelationRDD.getNumConstraintsFromFile(Paths.get(dirName, "r1cs").toString());
    }

}