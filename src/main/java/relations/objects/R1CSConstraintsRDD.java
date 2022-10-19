/* @file
 *****************************************************************************
 * @author     This file is part of zkspark, developed by SCIPR Lab
 *             and contributors (see AUTHORS).
 * @copyright  MIT license (see LICENSE file)
 *****************************************************************************/

package relations.objects;

import algebra.fields.AbstractFieldElementExpanded;
import configuration.Configuration;
import scala.Tuple2;
import utils.Serialize;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;

/**
 * A system of R1CSRelation constraints looks like
 * <p>
 * { < A_k , X > * < B_k , X > = < C_k , X > }_{k=1}^{n}  .
 * <p>
 * In other words, the system is satisfied if and only if there exist a
 * USCS variable assignment for which each R1CSRelation constraint is satisfied.
 * <p>
 * NOTE:
 * The 0-th variable (i.e., "x_{0}") always represents the constant 1.
 * Thus, the 0-th variable is not included in num_variables.
 */
public class R1CSConstraintsRDD<FieldT extends AbstractFieldElementExpanded<FieldT>> implements
        Serializable {

    private JavaPairRDD<Long, LinearTerm<FieldT>> A;
    private JavaPairRDD<Long, LinearTerm<FieldT>> B;
    private JavaPairRDD<Long, LinearTerm<FieldT>> C;
    private long constraintSize;

    public R1CSConstraintsRDD(
            final JavaPairRDD<Long, LinearTerm<FieldT>> _A,
            final JavaPairRDD<Long, LinearTerm<FieldT>> _B,
            final JavaPairRDD<Long, LinearTerm<FieldT>> _C,
            final long _constraintSize) {
        A = _A;
        B = _B;
        C = _C;
        constraintSize = _constraintSize;
    }

    public JavaPairRDD<Long, LinearTerm<FieldT>> A() {
        return A;
    }

    public JavaPairRDD<Long, LinearTerm<FieldT>> B() {
        return B;
    }

    public JavaPairRDD<Long, LinearTerm<FieldT>> C() {
        return C;
    }

    public long size() {
        return constraintSize;
    }

    public void union(R1CSConstraintsRDD<FieldT> inputRDD) {
        A = A.union(inputRDD.A());
        B = B.union(inputRDD.B());
        C = C.union(inputRDD.C());
    }

    public void saveAsObjectFile(String dirName) throws IOException {
        File directory = new File(dirName);

        directory.mkdir();

        A.saveAsObjectFile(Paths.get(dirName, "A").toString());
        B.saveAsObjectFile(Paths.get(dirName, "B").toString());
        C.saveAsObjectFile(Paths.get(dirName, "C").toString());

        Serialize.SerializeObject(constraintSize, Paths.get(dirName, "constraintSize").toString());
    }

    public static <FieldT extends AbstractFieldElementExpanded<FieldT>> R1CSConstraintsRDD<FieldT> loadFromObjectFile(String dirName, Configuration config) throws IOException{
        final JavaRDD<Tuple2<Long, LinearTerm<FieldT>>> _ARDD = config.sparkContext().objectFile(Paths.get(dirName, "A").toString(), config.numPartitions());
        final JavaPairRDD<Long, LinearTerm<FieldT>> _A =  _ARDD.mapToPair(e -> e);

        final JavaRDD<Tuple2<Long, LinearTerm<FieldT>>> _BRDD = config.sparkContext().objectFile(Paths.get(dirName, "B").toString(), config.numPartitions());
        final JavaPairRDD<Long, LinearTerm<FieldT>> _B =  _BRDD.mapToPair(e -> e);

        final JavaRDD<Tuple2<Long, LinearTerm<FieldT>>> _CRDD = config.sparkContext().objectFile(Paths.get(dirName, "C").toString(), config.numPartitions());
        final JavaPairRDD<Long, LinearTerm<FieldT>> _C =  _CRDD.mapToPair(e -> e);

        final long _constraintSize = (long) Serialize.UnserializeObject(Paths.get(dirName, "constraintSize").toString());

        return new R1CSConstraintsRDD<FieldT>(_A, _B, _C, _constraintSize);
    }
}
