/* @file
 *****************************************************************************
 * @author     This file is part of zkspark, developed by SCIPR Lab
 *             and contributors (see AUTHORS).
 * @copyright  MIT license (see LICENSE file)
 *****************************************************************************/

package relations.r1cs;

import algebra.fields.AbstractFieldElementExpanded;
import configuration.Configuration;

import org.apache.spark.api.java.JavaPairRDD;
import relations.objects.Assignment;
import relations.objects.LinearCombination;
import relations.objects.LinearTerm;
import relations.objects.R1CSConstraint;
import relations.objects.R1CSConstraintsRDD;
import scala.Tuple2;
import utils.Serialize;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

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
 * <p>
 * A R1CSRelation variable assignment is a vector of <FieldT> elements that represents
 * a candidate solution to a R1CSRelation constraint system.
 */
public class R1CSRelationRDD<FieldT extends AbstractFieldElementExpanded<FieldT>> implements
        Serializable {

    private final R1CSConstraintsRDD<FieldT> constraints;

    private final int numInputs;
    private final long numAuxiliary;
    private final long numConstraints;

    public R1CSRelationRDD(
            final R1CSConstraintsRDD<FieldT> _constraints,
            final int _numInputs,
            final long _numAuxiliary) {
        constraints = _constraints;
        numInputs = _numInputs;
        numAuxiliary = _numAuxiliary;
        numConstraints = _constraints.size();
    }

    public R1CSRelationRDD(
            final R1CSRelation<FieldT> serialR1CS,
            final Configuration config) {

        numInputs = serialR1CS.numInputs();
        numAuxiliary = serialR1CS.numVariables() - numInputs;
        numConstraints = serialR1CS.numConstraints();

        List<Tuple2<Long, LinearTerm<FieldT>>> serialA = new ArrayList<Tuple2<Long, LinearTerm<FieldT>>>();
        List<Tuple2<Long, LinearTerm<FieldT>>> serialB = new ArrayList<Tuple2<Long, LinearTerm<FieldT>>>();
        List<Tuple2<Long, LinearTerm<FieldT>>> serialC = new ArrayList<Tuple2<Long, LinearTerm<FieldT>>>();

        for (int i = 0; i < serialR1CS.numConstraints(); i++) {
            R1CSConstraint<FieldT> constraint = serialR1CS.constraints(i);

            LinearCombination<FieldT> A = constraint.A();
            for (int j = 0; j < A.terms().size(); j++) {
                serialA.add(new Tuple2<Long,LinearTerm<FieldT>>(new Long(i), A.get(j)));
            }

            LinearCombination<FieldT> B = constraint.B();
            for (int j = 0; j < B.terms().size(); j++) {
                serialB.add(new Tuple2<Long,LinearTerm<FieldT>>(new Long(i), B.get(j)));
            }

            LinearCombination<FieldT> C = constraint.C();
            for (int j = 0; j < C.terms().size(); j++) {
                serialC.add(new Tuple2<Long,LinearTerm<FieldT>>(new Long(i), C.get(j)));
            }
        }

        constraints = new R1CSConstraintsRDD<>(config.sparkContext().parallelizePairs(serialA),
            config.sparkContext().parallelizePairs(serialB),
            config.sparkContext().parallelizePairs(serialC),
            numConstraints);
    }

    public boolean isValid() {
        if (this.numInputs() > this.numVariables()) {
            return false;
        }

        final long numVariables = this.numVariables();
        final long countA = constraints.A().filter(term -> term._2.index() > numVariables).count();
        final long countB = constraints.B().filter(term -> term._2.index() > numVariables).count();
        final long countC = constraints.C().filter(term -> term._2.index() > numVariables).count();

        return countA == 0 && countB == 0 && countC == 0;
    }

    public boolean isSatisfied(
            final Assignment<FieldT> primary,
            final JavaPairRDD<Long, FieldT> oneFullAssignment) {
        assert (oneFullAssignment.count() == this.numVariables());

        // Assert first element == FieldT.one().
        final FieldT firstElement = oneFullAssignment.lookup(0L).get(0);
        final FieldT one = primary.get(0).one();
        assert (firstElement.equals(one));
        assert (primary.get(0).equals(one));

        // Compute the assignment evaluation for each constraint.
        final JavaPairRDD<Long, FieldT> zeroIndexedA = constraints().A()
                .filter(e -> e._2.index() == 0)
                .mapValues(LinearTerm::value)
                .reduceByKey(FieldT::add);

        final JavaPairRDD<Long, FieldT> zeroIndexedB = constraints().B()
                .filter(e -> e._2.index() == 0)
                .mapValues(LinearTerm::value)
                .reduceByKey(FieldT::add);

        final JavaPairRDD<Long, FieldT> zeroIndexedC = constraints().C()
                .filter(e -> e._2.index() == 0)
                .mapValues(LinearTerm::value)
                .reduceByKey(FieldT::add);

        final JavaPairRDD<Long, FieldT> A = constraints().A().filter(e -> e._2.index() != 0)
                .mapToPair(term -> {
                    // Swap the constraint and term index positions.
                    return new Tuple2<>(term._2.index(), new Tuple2<>(term._1, term._2.value()));
                }).join(oneFullAssignment).mapToPair(term -> {
                    // Multiply the constraint value by the input value.
                    return new Tuple2<>(term._2._1._1, term._2._1._2.mul(term._2._2));
                }).union(zeroIndexedA).reduceByKey(FieldT::add);

        final JavaPairRDD<Long, FieldT> B = constraints().B().filter(e -> e._2.index() != 0)
                .mapToPair(term -> {
                    // Swap the constraint and term index positions.
                    return new Tuple2<>(term._2.index(), new Tuple2<>(term._1, term._2.value()));
                }).join(oneFullAssignment).mapToPair(term -> {
                    // Multiply the constraint value by the input value.
                    return new Tuple2<>(term._2._1._1, term._2._1._2.mul(term._2._2));
                }).union(zeroIndexedB).reduceByKey(FieldT::add);

        final JavaPairRDD<Long, FieldT> C = constraints().C().filter(e -> e._2.index() != 0)
                .mapToPair(term -> {
                    // Swap the constraint and term index positions.
                    return new Tuple2<>(term._2.index(), new Tuple2<>(term._1, term._2.value()));
                }).join(oneFullAssignment).mapToPair(term -> {
                    // Multiply the constraint value by the input value.
                    return new Tuple2<>(term._2._1._1, term._2._1._2.mul(term._2._2));
                }).union(zeroIndexedC).reduceByKey(FieldT::add);

        final long count = A.join(B).join(C).filter(term -> {
            final FieldT a = term._2._1._1;
            final FieldT b = term._2._1._2;
            final FieldT c = term._2._2;

            if (!a.mul(b).equals(c)) {
                System.out.println("R1CSConstraint unsatisfied (index " + term._1 + "):");
                System.out.println("<a,(1,x)> = " + a);
                System.out.println("<b,(1,x)> = " + b);
                System.out.println("<c,(1,x)> = " + c);
                return true;
            }

            return false;
        }).count();

        return count == 0;
    }


    public R1CSConstraintsRDD<FieldT> constraints() {
        return constraints;
    }

    public int numInputs() {
        return numInputs;
    }

    public long numVariables() {
        return numInputs + numAuxiliary;
    }

    public long numConstraints() {
        return numConstraints;
    }

    public void saveAsObjectFile(String dirName) throws IOException {
        File directory = new File(dirName);

        directory.mkdir();

        constraints.saveAsObjectFile(Paths.get(dirName, "constraints").toString());

        Serialize.SerializeObject(numInputs, Paths.get(dirName, "numInputs").toString());
        Serialize.SerializeObject(numAuxiliary, Paths.get(dirName, "numAuxiliary").toString());
        Serialize.SerializeObject(numConstraints, Paths.get(dirName, "numConstraints").toString());
    }

    public static <FieldT extends AbstractFieldElementExpanded<FieldT>> R1CSRelationRDD<FieldT> loadFromObjectFile(String dirName,Configuration config) throws IOException{
        final R1CSConstraintsRDD<FieldT> _constraints = R1CSConstraintsRDD.loadFromObjectFile(Paths.get(dirName, "constraints").toString(), config);
        final int _numInputs = (int) Serialize.UnserializeObject(Paths.get(dirName, "numInputs").toString());
        final long _numAuxiliary = (long) Serialize.UnserializeObject(Paths.get(dirName, "numAuxiliary").toString());

        return new R1CSRelationRDD<FieldT>(_constraints, _numInputs, _numAuxiliary);
    }

    public static long getNumConstraintsFromFile(String dirName) throws IOException {
        final long _numConstraints = (long) Serialize.UnserializeObject(Paths.get(dirName, "numConstraints").toString());
        return _numConstraints;
    }
}
