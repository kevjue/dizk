package utils;

import java.io.FileReader;
import java.io.IOException;

import javax.print.event.PrintJobListener;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import algebra.curves.barreto_naehrig.bn254a.BN254aFields.BN254aFr;
import relations.objects.Assignment;
import relations.objects.LinearCombination;
import relations.objects.LinearTerm;
import relations.objects.R1CSConstraint;
import relations.objects.R1CSConstraints;
import relations.r1cs.R1CSRelation;
import scala.Tuple2;

public class Circom {
    public static R1CSRelation<BN254aFr> readR1CSFile(final FileReader r1csFile) throws IOException {
        final JsonReader jsonReader = new JsonReader(r1csFile);

        int numConstraints = 0;
        int numInputs = 1;  // Is nPubInputs + nOutputs + 1.  The +1 is for the constant "variable" within the constraints.
        int numAuxiliary = 0;   // Is nVars - numInputs (above java variable)
        final R1CSConstraints<BN254aFr> constraints = new R1CSConstraints<BN254aFr>();

        jsonReader.beginObject();
        while (jsonReader.hasNext()) {
            String name = jsonReader.nextName();

            if (name.equals("n8") || 
                    name.equals("prime") || 
                    name.equals("nPrvInputs") || 
                    name.equals("nLabels") || 
                    name.equals("useCustomGates") ||
                    name.equals("customGates") ||
                    name.equals("customGatesUses") ||
                    name.equals("map")) {
                // Ignore this entry
                jsonReader.skipValue();
            } else if (name.equals("nConstraints")) {
                numConstraints = jsonReader.nextInt();
            } else if (name.equals("nPubInputs")) {
                final int value = jsonReader.nextInt();
                numInputs += value;
                numAuxiliary -= value;
            } else if (name.equals("nOutputs")) {
                final int value = jsonReader.nextInt();
                numInputs += value;
                numAuxiliary -= value;
            } else if (name.equals("nVars")) {
                numAuxiliary += jsonReader.nextInt();
            } else if (name.equals("constraints")) {
                jsonReader.beginObject();

                while (jsonReader.hasNext()) {
                    name = jsonReader.nextName();

                    if (name.equals("length")) {
                        jsonReader.skipValue();
                    } else if (name.equals("arr")) {

                        jsonReader.beginArray();
                        while (jsonReader.hasNext()) {

                            if (jsonReader.peek() == JsonToken.NULL) {
                                jsonReader.nextNull();
                                continue;
                            }

                            jsonReader.beginArray();
                            while (jsonReader.hasNext())
                            {
                                if (jsonReader.peek() == JsonToken.NULL) {
                                    jsonReader.nextNull();
                                    continue;
                                }

                                // Each entry in the constraints array will have 3 arrays of objects, for the A,B,C constraints.
                                // Each object will be key-value entries for all non zero the contraint.
                                jsonReader.beginArray();
            
                                // Read the A array
                                LinearCombination<BN254aFr> aLinearCombination = new LinearCombination<BN254aFr>();
                                LinearCombination<BN254aFr> bLinearCombination = new LinearCombination<BN254aFr>();
                                LinearCombination<BN254aFr> cLinearCombination = new LinearCombination<BN254aFr>();
    
                                LinearCombination<BN254aFr> lc;
                                int objectIdx = 0;
                                while (jsonReader.hasNext()) {
                                    if (objectIdx == 0) {   // The A constraints
                                        lc = aLinearCombination;
                                    } else if (objectIdx == 1) {  // The B constraints
                                        lc = bLinearCombination;
                                    } else {    // The C constraints
                                        lc = cLinearCombination;
                                    }

                                    // Each element in the array is an object
                                    jsonReader.beginObject();
            
                                    String constraintNum;
                                    String constraintVal;
                                    while (jsonReader.hasNext()) {
                                        constraintNum = jsonReader.nextName();
                                        constraintVal = jsonReader.nextString();
            
                                        int index = Integer.parseInt(constraintNum);
                                        BN254aFr value = new BN254aFr(constraintVal);
            
                                        lc.add(new LinearTerm<BN254aFr>(index, value));
                                    }
                                    jsonReader.endObject();
                                    objectIdx++;
                                }            
                                R1CSConstraint<BN254aFr> constraint = new R1CSConstraint<BN254aFr>(aLinearCombination, bLinearCombination, cLinearCombination);
                                constraints.add(constraint);
    
                                if (constraints.size() % 100000 == 0) {
                                    System.out.println("constraints is " + constraints.size());
                                }
                                jsonReader.endArray();
                            }    
                            jsonReader.endArray();
                        }

                        jsonReader.endArray();
                        System.out.println("finished parsing arr");
                    }
                }
                System.out.println("finished parsing constraints object");
                jsonReader.endObject();
            }
        }
        jsonReader.close();

        System.out.println("num constraints is " + numConstraints);
        System.out.println("constraints array length is " + constraints.size());
        System.out.println("inputs length is " + numInputs);
        System.out.println("auxiliary length is " + numAuxiliary);

        R1CSRelation<BN254aFr> r1cs = new R1CSRelation<BN254aFr>(constraints, numInputs, numAuxiliary);

        return r1cs;
    }

    public static Tuple2<Assignment<BN254aFr>, Assignment<BN254aFr>> readWitnessFile(final FileReader wtnsFile, final int numInputs) {
        final JsonParser parser = new JsonParser();
        final JsonArray witness = (JsonArray) parser.parse(wtnsFile);

        // Primary will contain the public input
        Assignment<BN254aFr> primary = new Assignment<BN254aFr>();

        // Auxilary contains the private witness
        Assignment<BN254aFr> auxilary = new Assignment<BN254aFr>();

        // Note that the first element in the witness array is for the constant "variable" in the contraints.
        // It should always be 1.
        for (int i = 0; i < witness.size(); i++) {
            BN254aFr val = new BN254aFr(witness.get(i).getAsBigInteger());

            if (i < numInputs) {
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
