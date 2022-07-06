package utils;

import java.io.FileReader;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import algebra.curves.barreto_naehrig.bn254a.BN254aFields.BN254aFr;
import relations.objects.Assignment;
import relations.objects.LinearCombination;
import relations.objects.LinearTerm;
import relations.objects.R1CSConstraints;
import relations.objects.R1CSConstraint;
import relations.r1cs.R1CSRelation;
import scala.Tuple2;

public class Circom {
    public static R1CSRelation<BN254aFr> readR1CSFile(final FileReader r1csFile) {
        final JsonParser jsonParser = new JsonParser();
        final JsonObject jsonObj = (JsonObject) jsonParser.parse(r1csFile);

        final int numConstraints = jsonObj.get("nConstraints").getAsInt();

        // The +1 is for the constant "variable" within the constraints.  That value will be the first element
        // in the witness file and will always have the value of 1.
        final int numInputs = jsonObj.get("nPubInputs").getAsInt() + jsonObj.get("nOutputs").getAsInt() + 1;

        final int numAuxiliary = jsonObj.get("nVars").getAsInt() - numInputs;

        final R1CSConstraints<BN254aFr> constraints = new R1CSConstraints<BN254aFr>();
        final JsonArray jsonConstraints = jsonObj.get("constraints").getAsJsonArray();

        System.out.println("r1cs file nConstraints is " + numConstraints);
        for (int i = 0; i < numConstraints; i++) {
            JsonArray jsonConstraint = jsonConstraints.get(i).getAsJsonArray();
            JsonObject AJsonObj = (JsonObject) jsonConstraint.get(0);
            JsonObject BJsonObj = (JsonObject) jsonConstraint.get(1);
            JsonObject CJsonObj = (JsonObject) jsonConstraint.get(2);

            Set<Entry<String,JsonElement>> aEntries = AJsonObj.entrySet();
            Set<Entry<String,JsonElement>> bEntries = BJsonObj.entrySet();
            Set<Entry<String,JsonElement>> cEntries = CJsonObj.entrySet();

            LinearCombination<BN254aFr> aLinearCombination = new LinearCombination<BN254aFr>();
            for (Iterator<Entry<String,JsonElement>> iter = aEntries.iterator(); iter.hasNext(); ) {
                Entry<String,JsonElement> entry = iter.next();
                int index = Integer.parseInt(entry.getKey());
                BN254aFr value = new BN254aFr(entry.getValue().getAsBigInteger());

                aLinearCombination.add(new LinearTerm<BN254aFr>(index, value));
            }

            LinearCombination<BN254aFr> bLinearCombination = new LinearCombination<BN254aFr>();
            for (Iterator<Entry<String,JsonElement>> iter = bEntries.iterator(); iter.hasNext(); ) {
                Entry<String,JsonElement> entry = iter.next();
                int index = Integer.parseInt(entry.getKey());
                BN254aFr value = new BN254aFr(entry.getValue().getAsBigInteger());

                bLinearCombination.add(new LinearTerm<BN254aFr>(index, value));
            }

            LinearCombination<BN254aFr> cLinearCombination = new LinearCombination<BN254aFr>();
            for (Iterator<Entry<String,JsonElement>> iter = cEntries.iterator(); iter.hasNext(); ) {
                Entry<String,JsonElement> entry = iter.next();
                int index = Integer.parseInt(entry.getKey());
                BN254aFr value = new BN254aFr(entry.getValue().getAsBigInteger());

                cLinearCombination.add(new LinearTerm<BN254aFr>(index, value));
            }

            R1CSConstraint<BN254aFr> constraint = new R1CSConstraint<BN254aFr>(aLinearCombination, bLinearCombination, cLinearCombination);
            constraints.add(constraint);
        }

        System.out.println("constraints length is " + constraints.size());
        System.out.println("inputs length is " + numInputs);
        System.out.println("auxiliary length is " + numAuxiliary);

        R1CSRelation<BN254aFr> r1cs = new R1CSRelation<BN254aFr>(constraints, numInputs, numAuxiliary);

        Assignment<BN254aFr> primary = new Assignment<BN254aFr>();
        primary.add(new BN254aFr("1"));
        primary.add(new BN254aFr("12201"));
        primary.add(new BN254aFr("1"));

        Assignment<BN254aFr> auxilary = new Assignment<BN254aFr>();
        auxilary.add(new BN254aFr("2"));
        auxilary.add(new BN254aFr("3"));
        auxilary.add(new BN254aFr("4"));
        auxilary.add(new BN254aFr("83"));
        auxilary.add(new BN254aFr("5"));
        auxilary.add(new BN254aFr("16"));
        auxilary.add(new BN254aFr("147"));
        auxilary.add(new BN254aFr("6"));
        auxilary.add(new BN254aFr("24"));

        System.out.println("isSatisfied is " + r1cs.isSatisfied(primary, auxilary));

        return r1cs;
    }

    public static Tuple2<Assignment<BN254aFr>, Assignment<BN254aFr>> readWitnessFile(final FileReader wtnsFile, final int numInputs) {
        final JsonParser parser = new JsonParser();
        final JsonArray witness = (JsonArray) parser.parse(wtnsFile);
        Assignment<BN254aFr> primary = new Assignment<BN254aFr>();
        Assignment<BN254aFr> auxilary = new Assignment<BN254aFr>();

        // Note that the first element in the witness array is for the constant "variable" in the contraints.
        // It should always be 0.
        for (int i = 0; i < witness.size(); i++) {
            BN254aFr val = new BN254aFr(witness.get(i).getAsBigInteger());

            if (i < (numInputs)) {
                primary.add(val);
            } else {
                auxilary.add(val);
            }
        }

        return new Tuple2<Assignment<BN254aFr>, Assignment<BN254aFr>> (primary, auxilary);
    }

    public static Assignment<BN254aFr> readPublicInputFile(final FileReader publicInputFile) {
        final JsonParser parser = new JsonParser();
        final JsonArray publicInputJSON = (JsonArray) parser.parse(publicInputFile);
        Assignment<BN254aFr> publicInput = new Assignment<BN254aFr>();

        // Add in 1 for the "constant" variable.
        publicInput.add(BN254aFr.ONE);
        for (int i = 0; i < publicInputJSON.size(); i++) {
            BN254aFr val = new BN254aFr(publicInputJSON.get(i).getAsBigInteger());

            publicInput.add(val);
        }

        return publicInput;
    }

    /* public static CRS<BN254aFr, BN254aG1, BN254aG2, BN254aGT> readZkeyFile(final File zkeyFile, R1CSRelation<BN254aFr> r1cs) {
        final JsonParser parser = new JsonParser();
        final JsonObject jsonObj = (JsonObject) parser.parse(zkeyFile);        

        final List<G1T> _deltaABCG1;
        final List<G1T> _queryH,

        final BN254aG1 alphaG1 = getG1Element(jsonObj.get("vk_alpha_1").getAsJsonArray());
        final BN254aG1 betaG1 = getG1Element(jsonObj.get("vk_beta_1").getAsJsonArray());
        final BN254aG2 betaG2 = getG2Element(jsonObj.get("vk_beta_2").getAsJsonArray());
        final BN254aG1 deltaG1 = getG1Element(jsonObj.get("vk_delta_1").getAsJsonArray());
        final BN254aG2 deltaG2 = getG2Element(jsonObj.get("vk_delta_2").getAsJsonArray());

        final List<BN254aG1> queryA = new ArrayList<BN254aG1>();
        JsonArray AJson = jsonObj.get("A").getAsJsonArray();
        for (int i = 0; i < AJson.size(); i++) {
            queryA.add(getG1Element(AJson.get(i).getAsJsonArray()));
        }
        
        final List<Tuple2<BN254aG1, BN254aG2>> queryB = new ArrayList<Tuple2<BN254aG1, BN254aG2>>();
        JsonArray B1Json = jsonObj.get("B1").getAsJsonArray();
        JsonArray B2Json = jsonObj.get("B2").getAsJsonArray();

        for (int i = 0; i < B1Json.size(); i++) {
            BN254aG1 B1 = getG1Element(B1Json.get(i).getAsJsonArray());
            BN254aG2 B2 = getG2Element(B2Json.get(i).getAsJsonArray());
            
            Tuple2<BN254aG1, BN254aG2> BElement = new Tuple2<BN254aG1, BN254aG2>(B1, B2);

            queryB.add(BElement);
        }

        ProvingKey<BN254aFr, BN254aG1, BN254aG2> provingKey = new ProvingKey<BN254aFr, BN254aG1, BN254aG2>();
        
        return new ProvingKey<BN254aFr, BN254aG1, BN254aG2>(null, null, null, null, null, null, null, null, null, null);
    }

    private static BN254aG1 getG1Element(final JsonArray jsonArray) {
        final BN254aFq X = new BN254aFq(jsonArray.get(0).getAsBigInteger());
        final BN254aFq Y = new BN254aFq(jsonArray.get(1).getAsBigInteger());
        final BN254aFq Z = new BN254aFq(jsonArray.get(2).getAsBigInteger());
        
        return new BN254aG1(X, Y, Z);
    }

    private static BN254aG2 getG2Element(final JsonArray jsonArray) {
        final JsonArray XJson = jsonArray.get(0).getAsJsonArray();
        final JsonArray YJson = jsonArray.get(1).getAsJsonArray();
        final JsonArray ZJson = jsonArray.get(2).getAsJsonArray();

        final BN254aFq2 X = new BN254aFq2(XJson.get(0).getAsBigInteger(), XJson.get(1).getAsBigInteger());
        final BN254aFq2 Y = new BN254aFq2(YJson.get(0).getAsBigInteger(), YJson.get(1).getAsBigInteger());
        final BN254aFq2 Z = new BN254aFq2(ZJson.get(0).getAsBigInteger(), ZJson.get(1).getAsBigInteger());
        
        return new BN254aG2(X, Y, Z);
    } */
}
