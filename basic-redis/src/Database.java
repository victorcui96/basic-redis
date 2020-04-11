import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.Scanner;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Database {

    private Map<String, Integer> hashTable;
    private static final String nil = "<nil>";
    private static final String key = "key";
    private static final String anotherKey = "anotherKey";
    private static final String NOT_IN_TRANSACTION = "NOT IN TRANSACTION";

    public Database() {
        this.hashTable = new HashMap<>();
    }

    public Optional<Integer> getValueOfKey(final String key) {
        if (!this.hashTable.containsKey(key)) {
            return Optional.empty();
        } else {
            return Optional.of(this.hashTable.get(key));
        }
    }

    public void setValueForKey(final String key, final int value) {
        this.hashTable.put(key, value);
    }

    public void incrementValueForKey(final String key) {
        if (!this.hashTable.containsKey(key)) {
            setValueForKey(key, 0);
        }
        setValueForKey(key, getValueOfKey(key).get() + 1);
    }

    public void deleteKey(final String key) {
        this.hashTable.remove(key);
    }

    public void deleteValue(final int value) {
        this.hashTable.values().removeAll(Collections.singleton(value));
    }

    public int getSize() {
        return this.hashTable.size();
    }


    @Test
    void GIVEN_nonExistentKey_WHEN_getValue_THEN_EmptyOptional() {
        Database db = new Database();
        Optional<Integer> value = db.getValueOfKey(key);
        assertEquals(Optional.empty(), value);
    }

    @Test
    void GIVEN_existingKey_WHEN_getValue_THEN_valueReturned() {
        Database db = new Database();
        db.setValueForKey(key, 7);
        assertEquals(7, db.getValueOfKey(key).get());
    }

    @Test
    void GIVEN_nonExistentKey_WHEN_incrementValueForKey_THEN_valueSetToZero() {
        Database db = new Database();
        db.incrementValueForKey(key);
        assertEquals(1, db.getValueOfKey(key).get());
    }

    @Test
    void GIVEN_existingKey_WHEN_incrementValueForKey_THEN_valueIncremented() {
        Database db = new Database();
        db.setValueForKey(key, 1);
        db.incrementValueForKey(key);
        assertEquals(2, db.getValueOfKey(key).get());
    }

    @Test
    void GIVEN_deleteExistingKey_WHEN_deleteKey_THEN_keyRemoved() {
        Database db = new Database();
        db.setValueForKey(key, new Random().nextInt(5));
        db.deleteKey(key);
        assertEquals(Optional.empty(), db.getValueOfKey(key));
    }

    @Test
    void GIVEN_deleteNonExistentKey_WHEN_deleteKey_THEN_existingKeysUnaffected() {
        Database db = new Database();
        int randomValue1 = new Random().nextInt(5);
        int randomValue2 = new Random().nextInt(10);
        db.setValueForKey(key, randomValue1);
        db.setValueForKey(anotherKey, randomValue2);
        db.deleteKey("fakeKey");
        assertEquals(randomValue1, db.getValueOfKey(key).get());
        assertEquals(randomValue2, db.getValueOfKey(anotherKey).get());
    }

    @Test
    void GIVEN_nonExistentValue_WHEN_removeAllKeysWithValue_THEN_databaseUnchanged() {
        Database db = new Database();
        int randomValue1 = new Random().nextInt(5);
        int randomValue2 = new Random().nextInt(10);
        db.setValueForKey(key, randomValue1);
        db.setValueForKey(anotherKey, randomValue2);
        db.deleteValue(1000);
        assertEquals(2, db.getSize());
        assertEquals(randomValue1, db.getValueOfKey(key).get());
        assertEquals(randomValue2, db.getValueOfKey(anotherKey).get());
    }

    @Test
    void GIVEN_existingValue_WHEN_removeAllKeysWithValue_THEN_AllKeysWithValueRemoved() {
        Database db = new Database();
        int randomValue1 = 1000;
        int randomValue2 = 10;
        db.setValueForKey(key, randomValue1);
        db.setValueForKey(anotherKey, randomValue1);
        db.setValueForKey("thirdKey", randomValue1);
        db.setValueForKey("lebronJames", randomValue2);
        db.deleteValue(randomValue1);
        assertEquals(1, db.getSize());
        assertEquals(db.getValueOfKey("lebronJames").get(), randomValue2);
    }


    public static void handleGetCommand(final String key, final Optional<Integer> value) {
        if (value.isEmpty()) {
            System.out.println(nil);
        } else {
            System.out.println(value.get());
        }
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        Database db = new Database();
        while (true) {
            // specification didn't specify a quit character, so infinite loop until user quits the program
            String line = scanner.nextLine();
            String command = line.split(" ")[0];
            switch (command) {
                case "GET":
                    String keyForGet = line.split(" ")[1];
                    Optional<Integer> valueForGet = db.getValueOfKey(keyForGet);
                    handleGetCommand(keyForGet, valueForGet);
                    break;
                case "SET":
                    String keyForSet = line.split(" ")[1];
                    int valueForSet = Integer.parseInt(line.split(" ")[2]);
                    db.setValueForKey(keyForSet, valueForSet);
                    break;
                case "DEL":
                    String keyForDelete = line.split(" ")[1];
                    db.deleteKey(keyForDelete);
                    break;
                case "INCR":
                    String keyToIncrement = line.split(" ")[1];
                    db.incrementValueForKey(keyToIncrement);
                    break;
                case "DELVALUE":
                    int valueToDelete = Integer.parseInt(line.split(" ")[1]);
                    db.deleteValue(valueToDelete);
                    break;
                case "MULTI":
                    // it seems like once a "MULTI" is encountered, every command after should be considered
                    // as part of the MULTI block
                    while (true) {
                        Queue<String> transactions = new LinkedList<>();
                        String atomicTransaction;
                        String atomicCommand;
                        do {
                            atomicTransaction = scanner.nextLine();
                            atomicCommand = atomicTransaction.split(" ")[0];
                            if (!atomicCommand.equals("GET") && !atomicCommand.equals("DISCARD") && !atomicCommand.equals("EXEC")) {
                                // "GET, DISCARD, and EXEC" should not be counted as transactions
                                transactions.add(atomicTransaction);
                            }
                            // It seems like we still want to execute transactions in our database here (according to example 3),
                            // in addition to executing the commands in a transaction when we see the EXEC keyword. I'm unsure of this part and the
                            // specification is ambigious.
                            if (atomicCommand.equals("GET")) {
                                String queuedKey = atomicTransaction.split(" ")[1];
                                Optional<Integer> queuedValue = db.getValueOfKey(queuedKey);
                                handleGetCommand(queuedKey, queuedValue);
                            } else if (atomicCommand.equals("SET")) {
                                String queuedKeyForSet = atomicTransaction.split(" ")[1];
                                int queuedValueForSet = Integer.parseInt(atomicTransaction.split(" ")[2]);
                                db.setValueForKey(queuedKeyForSet, queuedValueForSet);
                            } else if (atomicCommand.equals("DEL")) {
                                String queuedKeyForDelete = atomicTransaction.split(" ")[1];
                                db.deleteKey(queuedKeyForDelete);
                            } else if (atomicCommand.equals("INCR")) {
                                String queuedKeyToIncrement = atomicTransaction.split(" ")[1];
                                db.incrementValueForKey(queuedKeyToIncrement);
                            } else if (atomicCommand.equals("DELVALUE")) {
                                int queuedValToDelete = Integer.parseInt(atomicTransaction.split(" ")[1]);
                                db.deleteValue(queuedValToDelete);
                            }
                        } while (!atomicCommand.equals("EXEC") && !atomicCommand.equals("DISCARD"));
                        if (atomicCommand.equals("DISCARD")) {
                            // How can we discard commands if they've already been executed in the database? Sample 3
                            // seems ambigious. In addition, in the examples under MULTI value
                            // it says "The MULTI command is followed by zero or more commands before
                            // EXEC or DISCARD". Yet Sample 5 and Sample 6 have the EXEC and DISCARD commands before any MULTI commands.
                            if (transactions.size() == 0) {
                                // no transactions were started
                                System.out.println(NOT_IN_TRANSACTION);
                            } else {
                                System.out.println(transactions.size());
                                transactions.clear();
                            }
                        } else if (atomicCommand.equals("EXEC")) {
                            if (transactions.size() == 0) {
                                System.out.println(NOT_IN_TRANSACTION);
                            } else {
                                // print out the number of transactions executed
                                System.out.println(transactions.size());
                                transactions.clear();
                            }
                        }
                    }
            }
        }
    }
}
