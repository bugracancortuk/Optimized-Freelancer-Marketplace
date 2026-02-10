import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;

/**
 * Custom Comparator Interface.
 */
interface Comparator<T> {
    int compare(T a, T b);
}

/**
 * Entry structure for the Hash Map.
 */
class MapEntry<V> {
    final String key;
    V value;

    MapEntry(String key, V value) {
        this.key = key;
        this.value = value;
    }
}

/**
 * Custom Hash Map replacement using ArrayList and LinkedList (Chaining for O(1) average).
 */
class FastHashMap<V> {
    private final ArrayList<LinkedList<MapEntry<V>>> table;
    private int size;
    private static final int DEFAULT_CAPACITY = 65536; // 2^16

    public FastHashMap(int capacity) {
        int effectiveCapacity = 1;
        while (effectiveCapacity < capacity) {
            effectiveCapacity <<= 1;
        }

        this.table = new ArrayList<>(effectiveCapacity);
        for(int i = 0; i < effectiveCapacity; i++) {
            this.table.add(null);
        }
        this.size = 0;
    }

    public FastHashMap() {
        this(DEFAULT_CAPACITY);
    }

    private int hash(String key) {
        int h = key.hashCode();
        return (h ^ (h >>> 16)) & (table.size() - 1); // Simple and fast hash function
    }

    public V put(String key, V value) {
        int index = hash(key);
        LinkedList<MapEntry<V>> bucket = table.get(index);

        if (bucket == null) {
            bucket = new LinkedList<>();
            table.set(index, bucket);
        }

        // Check for existing key and update
        for (MapEntry<V> entry : bucket) {
            if (entry.key.equals(key)) {
                V oldValue = entry.value;
                entry.value = value;
                return oldValue;
            }
        }

        // Key not found, add new entry
        MapEntry<V> newEntry = new MapEntry<>(key, value);
        bucket.add(newEntry);
        size++;
        return null;
    }

    public V get(String key) {
        int index = hash(key);
        LinkedList<MapEntry<V>> bucket = table.get(index);

        if (bucket == null) {
            return null;
        }

        for (MapEntry<V> entry : bucket) {
            if (entry.key.equals(key)) {
                return entry.value;
            }
        }
        return null;
    }

    public V remove(String key) {
        int index = hash(key);
        LinkedList<MapEntry<V>> bucket = table.get(index);

        if (bucket == null) {
            return null;
        }

        MapEntry<V> entryToRemove = null;
        for (MapEntry<V> entry : bucket) {
            if (entry.key.equals(key)) {
                entryToRemove = entry;
                break;
            }
        }

        if (entryToRemove != null) {
            V value = entryToRemove.value;
            bucket.remove(entryToRemove);
            size--;
            return value;
        }

        return null;
    }

    public boolean containsKey(String key) {
        return get(key) != null;
    }

    public int size() {
        return size;
    }

    public ArrayList<V> valuesList() {
        ArrayList<V> values = new ArrayList<>(size);
        for (LinkedList<MapEntry<V>> bucket : table) {
            if (bucket != null) {
                for (MapEntry<V> entry : bucket) {
                    values.add(entry.value);
                }
            }
        }
        return values;
    }

    public V computeIfAbsent(String key, Main.ServiceFactory factory) {
        V value = get(key);
        if (value == null) {
            value = (V) factory.create(key);
            put(key, value);
        }
        return value;
    }
}

/**
 * Custom Set replacement using FastHashMap (O(1) average).
 */
class FastHashSet {
    private final FastHashMap<Object> map;
    private static final Object PRESENT = new Object();

    public FastHashSet(int capacity) {
        this.map = new FastHashMap<>(capacity);
    }

    public boolean add(String key) {
        return map.put(key, PRESENT) == null;
    }

    public boolean remove(String key) {
        return map.remove(key) != null;
    }

    public boolean contains(String key) {
        return map.containsKey(key);
    }

    public int size() {
        return map.size();
    }

    public FastHashSet getSet() {
        return this;
    }
}

/**
 * Custom Sorting Implementation for ArrayList (O(N log N) Quick Sort).
 */
class ListSorter {
    public static <T> void sort(ArrayList<T> list, Comparator<T> c) {
        if (list.size() <= 1) return;
        quickSort(list, c, 0, list.size() - 1);
    }

    private static <T> void quickSort(ArrayList<T> list, Comparator<T> c, int low, int high) {
        if (low < high) {
            int pi = partition(list, c, low, high);
            quickSort(list, c, low, pi - 1);
            quickSort(list, c, pi + 1, high);
        }
    }

    private static <T> int partition(ArrayList<T> list, Comparator<T> c, int low, int high) {
        // Pivot selection: the last element
        T pivot = list.get(high);
        int i = (low - 1);

        for (int j = low; j < high; j++) {
            // If current element is "smaller" than or equal to pivot
            if (c.compare(list.get(j), pivot) <= 0) {
                i++;
                // swap list[i] and list[j]
                T temp = list.get(i);
                list.set(i, list.get(j));
                list.set(j, temp);
            }
        }

        // swap list[i+1] (the first element greater than pivot) and list[high] (pivot)
        T temp = list.get(i + 1);
        list.set(i + 1, list.get(high));
        list.set(high, temp);

        return i + 1;
    }
}

/**
 * Custom class for fast reading input, using a large buffer (1MB).
 */
class FastReader {
    private final InputStream is;
    private final byte[] buffer = new byte[1 << 20]; // 1MB buffer
    private int ptr = 0;
    private int buflen = 0;
    private static final byte CR = (byte) '\r';
    private static final byte LF = (byte) '\n';
    private static final byte SPACE = (byte) ' ';
    private static final byte TAB = (byte) '\t';
    private final StringBuilder tokenBuilder = new StringBuilder(128);

    public FastReader(InputStream is) {
        this.is = is;
    }

    /**
     * Reads the next byte from the buffer, refilling it if necessary.
     */
    private int read() throws IOException {
        if (buflen == ptr) {
            buflen = is.read(buffer);
            if (buflen <= 0) return -1;
            ptr = 0;
        }
        return buffer[ptr++];
    }

    /**
     * Skips whitespace characters.
     */
    private int skipWhitespace() throws IOException {
        int b = read();
        while (b != -1 && (b == SPACE || b == TAB || b == CR || b == LF)) {
            b = read();
        }
        return b;
    }

    /**
     * Reads the next non-whitespace token.
     */
    public String nextToken() throws IOException {
        int b = skipWhitespace();
        if (b == -1) return null;

        tokenBuilder.setLength(0);
        do {
            if (b == SPACE || b == TAB || b == CR || b == LF) {
                break;
            }
            tokenBuilder.append((char) b);
        } while ((b = read()) != -1);

        return tokenBuilder.toString();
    }

    public void close() throws IOException {
        if (is != null) {
            is.close();
        }
    }
}

/**
 * Custom class for fast writing output, using a large buffer (1MB).
 */
class FastWriter {
    private final OutputStream os;
    private final byte[] buffer = new byte[1 << 20]; // 1MB
    private int ptr = 0;
    private final byte[] lineSeparator = "\n".getBytes();

    public FastWriter(OutputStream os) {
        this.os = os;
    }

    /**
     * Writes a block of bytes to the buffer or directly to the stream if too large.
     */
    private void writeBytes(byte[] b, int off, int len) throws IOException {
        if (len >= buffer.length) {
            flush();
            os.write(b, off, len);
            return;
        }
        if (ptr + len >= buffer.length) {
            flush();
        }
        System.arraycopy(b, off, buffer, ptr, len);
        ptr += len;
    }

    /**
     * Writes a string to the output buffer.
     */
    public void write(String s) throws IOException {
        int len = s.length();
        if (len == 0) return;
        if (ptr + len >= buffer.length) {
            flush();
            if (len >= buffer.length) {
                writeBytes(s.getBytes(), 0, len);
                return;
            }
        }
        for (int i = 0; i < len; i++) {
            buffer[ptr++] = (byte) s.charAt(i);
        }
    }

    /**
     * Writes a newline character.
     */
    public void newLine() throws IOException {
        writeBytes(lineSeparator, 0, lineSeparator.length);
    }

    /**
     * Flushes the buffer to the underlying output stream.
     */
    public void flush() throws IOException {
        if (ptr > 0) {
            os.write(buffer, 0, ptr);
            ptr = 0;
        }
    }

    public void close() throws IOException {
        flush();
        if (os != null) {
            os.close();
        }
    }
}

/**
 * Manages a group of Freelancers for a specific service, maintaining
 * both the full list and an O(1) updatable list of available freelancers.
 */
class ServiceGroup {
    Freelancer[] list;
    int size;
    // Separate array for available freelancers only
    Freelancer[] availableList;
    int availableSize;
    private static final int INITIAL_CAPACITY = 16384;

    ServiceGroup() {
        list = new Freelancer[INITIAL_CAPACITY];
        availableList = new Freelancer[INITIAL_CAPACITY];
    }

    // Helper for array resizing and copying (Manual implementation)
    private Freelancer[] resize(Freelancer[] original, int newCapacity, int currentSize) {
        Freelancer[] newArray = new Freelancer[newCapacity];
        System.arraycopy(original, 0, newArray, 0, currentSize);
        return newArray;
    }

    /**
     * Adds a Freelancer to the main list and the available list (if available).
     */
    void add(Freelancer f) {
        if (size == list.length) {
            list = resize(list, size << 1, size);
        }

        // Ensure availableList is big enough if the freelancer is available
        if (f.available && availableSize == availableList.length) {
            availableList = resize(availableList, availableList.length << 1, availableSize);
        }

        f.positionInService = size;
        list[size++] = f;

        // Add to available list if they are currently available
        if (f.available) {
            notifyAvailable(f);
        }
    }

    /**
     * Removes a Freelancer from the main list and available list in O(1) time
     * using swap-with-last and size decrement.
     */
    void remove(Freelancer f) {
        if (f.available) {
            notifyUnavailable(f); // Remove from available list in O(1)
        }

        // Fast removal from the main list (O(1) with swap)
        int i = f.positionInService;
        if (i < 0 || i >= size) return;

        list[i] = list[--size];
        if (i < size) {
            list[i].positionInService = i;
        }
        f.positionInService = -1;
        list[size] = null; // Clean up last element
    }

    /**
     * O(1) method: Adds a Freelancer to the available list.
     */
    void notifyAvailable(Freelancer f) {
        if (availableSize == availableList.length) {
            availableList = resize(availableList, availableList.length << 1, availableSize);
        }
        f.positionInAvailableList = availableSize;
        availableList[availableSize++] = f;
    }

    /**
     * O(1) method: Removes a Freelancer from the available list using swap-with-last.
     */
    void notifyUnavailable(Freelancer f) {
        int i = f.positionInAvailableList;
        if (i < 0 || i >= availableSize) return;

        availableList[i] = availableList[--availableSize];
        if (i < availableSize) {
            availableList[i].positionInAvailableList = i;
        }
        f.positionInAvailableList = -1;
        availableList[availableSize] = null; // Clean up last element
    }
}

/**
 * Represents a Customer with loyalty and spending tracking.
 */
class Customer {
    String customerID;
    double totalAmountSpent;
    double totalLoyaltyBase;
    int cancellationCount;
    int totalEmploymentCount;
    String loyaltyTier;

    Customer(String id) {
        this.customerID = id;
        this.loyaltyTier = "BRONZE";
    }

    /**
     * Updates the customer's loyalty tier based on net spending.
     */
    void updateLoyaltyTier() {
        double eval = totalAmountSpent - (cancellationCount * 250);
        if (eval >= 5000) loyaltyTier = "PLATINUM";
        else if (eval >= 2000) loyaltyTier = "GOLD";
        else if (eval >= 500) loyaltyTier = "SILVER";
        else loyaltyTier = "BRONZE";
    }
}

/**
 * Represents a Freelancer with skills, job history, and availability status.
 */
class Freelancer {
    String freelancerID;
    String serviceName;
    double basePrice;
    int technicalProficiency;
    int communication;
    int creativity;
    int efficiency;
    int attentionToDetail;
    double rating;
    int completedJobs;
    int cancelledJobs;
    int monthlyCancellations;
    int monthlyCompletedJobs;
    boolean available;
    boolean burnout;
    String queuedService;
    double queuedPrice;
    int positionInService = -1;
    int positionInAvailableList = -1; // O(1) removal optimization

    // Cached composite score for current service
    int cachedCompositeScore = -1;
    boolean scoreNeedsUpdate = true;

    Freelancer(String id, String svc, double price, int T, int C, int R, int E, int A) {
        this.freelancerID = id;
        this.serviceName = svc;
        this.basePrice = price;
        this.technicalProficiency = T;
        this.communication = C;
        this.creativity = R;
        this.efficiency = E;
        this.attentionToDetail = A;
        this.rating = 5.0;
        this.available = true;
    }

    /**
     * Computes or retrieves the cached composite score for a given service.
     */
    int getOrComputeScore(Services service) {
        if (!scoreNeedsUpdate && cachedCompositeScore != -1) {
            return cachedCompositeScore;
        }

        int Ts = service.technicalProficiency;
        int Cs = service.communication;
        int Rs = service.creativity;
        int Es = service.efficiency;
        int As = service.attentionToDetail;
        double skillDenominator = service.skillDenominator;

        // Calculate skill score based on dot product of freelancer skills and service requirements
        double dotProduct = (double)(technicalProficiency * Ts + communication * Cs + creativity * Rs + efficiency * Es + attentionToDetail * As);
        double skillScore = dotProduct / skillDenominator;

        double ratingScore = rating / 5.0;

        int totalJobs = completedJobs + cancelledJobs;
        // Reliability is based on the proportion of completed jobs
        double reliabilityScore = (totalJobs == 0) ? 1.0 : 1.0 - ((double) cancelledJobs / totalJobs);

        double burnoutPenalty = burnout ? 0.45 : 0.0;

        // Final composite score calculation (weighted average with burnout penalty)
        cachedCompositeScore = (int) Math.floor(10000 * (0.55 * skillScore + 0.25 * ratingScore + 0.20 * reliabilityScore - burnoutPenalty));
        scoreNeedsUpdate = false;

        return cachedCompositeScore;
    }

    /**
     * Invalidates the cached composite score, forcing re-computation.
     */
    void invalidateScore() {
        scoreNeedsUpdate = true;
    }

    /**
     * Applies skill gain based on the service's skill requirements and the job rating.
     * Manual array copy ensures no unauthorized imports.
     */
    void gainSkillsFromJob(Services service, int rating) {
        if (rating < 4) return;

        int[] values = new int[5];
        values[0] = service.technicalProficiency;
        values[1] = service.communication;
        values[2] = service.creativity;
        values[3] = service.efficiency;
        values[4] = service.attentionToDetail;

        int[] topIndices = new int[]{-1, -1, -1};

        // Manual copy of values array (replaces Arrays.copyOf)
        int[] currentValues = new int[5];
        System.arraycopy(values, 0, currentValues, 0, 5);

        // Identify top 3 required skills for the service
        for (int i = 0; i < 3; i++) {
            int maxIdx = -1;
            int maxVal = -1;
            for (int j = 0; j < 5; j++) {
                if (currentValues[j] > maxVal) {
                    maxVal = currentValues[j];
                    maxIdx = j;
                }
            }
            if (maxIdx == -1 || maxVal <= 0) break;

            topIndices[i] = maxIdx;
            currentValues[maxIdx] = -1; // Mark as selected for the next iteration
        }

        // Apply skill gain to the top 3 required skills
        if (topIndices[0] != -1) applySkillGainByIndex(topIndices[0], 2);
        if (topIndices[1] != -1) applySkillGainByIndex(topIndices[1], 1);
        if (topIndices[2] != -1) applySkillGainByIndex(topIndices[2], 1);

        invalidateScore();
    }

    /**
     * Applies a uniform skill degradation (3 points).
     */
    void applySkillDegradation() {
        technicalProficiency = Math.min(100, Math.max(0, technicalProficiency - 3));
        communication = Math.min(100, Math.max(0, communication - 3));
        creativity = Math.min(100, Math.max(0, creativity - 3));
        efficiency = Math.min(100, Math.max(0, efficiency - 3));
        attentionToDetail = Math.min(100, Math.max(0, attentionToDetail - 3));
        invalidateScore();
    }

    /**
     * Helper to apply skill gain to a specific skill index.
     */
    void applySkillGainByIndex(int index, int gain) {
        switch (index) {
            case 0: technicalProficiency = Math.min(100, technicalProficiency + gain); break;
            case 1: communication = Math.min(100, communication + gain); break;
            case 2: creativity = Math.min(100, creativity + gain); break;
            case 3: efficiency = Math.min(100, efficiency + gain); break;
            case 4: attentionToDetail = Math.min(100, attentionToDetail + gain); break;
        }
    }
}

/**
 * Defines the skill requirements and weightings for a specific service type.
 */
class Services {
    String serviceName;
    int technicalProficiency;
    int communication;
    int creativity;
    int efficiency;
    int attentionToDetail;
    double skillDenominator;

    // Static arrays to replace HashMap for SERVICE_TYPES
    private static final String[] SERVICE_NAMES = {
            "paint", "web_dev", "graphic_design", "data_entry", "tutoring",
            "cleaning", "writing", "photography", "plumbing", "electrical"
    };
    private static final int[][] SERVICE_SKILLS = {
            {70, 60, 50, 85, 90},
            {95, 75, 85, 80, 90},
            {75, 85, 95, 70, 85},
            {50, 50, 30, 95, 95},
            {80, 95, 70, 90, 75},
            {40, 60, 40, 90, 85},
            {70, 85, 90, 80, 95},
            {85, 80, 90, 75, 90},
            {85, 65, 60, 90, 85},
            {90, 65, 70, 95, 95}
    };

    private static final FastHashMap<int[]> SERVICE_TYPES_MAP = initServiceMap();

    private static FastHashMap<int[]> initServiceMap() {
        FastHashMap<int[]> map = new FastHashMap<>(32);
        for (int i = 0; i < SERVICE_NAMES.length; i++) {
            map.put(SERVICE_NAMES[i], SERVICE_SKILLS[i]);
        }
        return map;
    }

    Services(String serviceName) {
        this.serviceName = serviceName;
        int[] skills = SERVICE_TYPES_MAP.get(serviceName);
        if (skills != null) {
            this.technicalProficiency = skills[0];
            this.communication = skills[1];
            this.creativity = skills[2];
            this.efficiency = skills[3];
            this.attentionToDetail = skills[4];
            this.skillDenominator = 100.0 * (skills[0] + skills[1] + skills[2] + skills[3] + skills[4]);
        }
    }

    static boolean isValidServiceType(String serviceName) {
        return SERVICE_TYPES_MAP.containsKey(serviceName);
    }
}

/**
 * Represents an active job employment between a Customer and a Freelancer.
 */
class Employment {
    Customer customer;
    Freelancer freelancer;

    Employment(Customer customer, Freelancer freelancer) {
        this.customer = customer;
        this.freelancer = freelancer;
    }
}

/**
 * Manages customer-specific blacklists of freelancers using FastHashMap and FastHashSet.
 */
class Blacklists {
    private FastHashMap<FastHashSet> blacklistMap = new FastHashMap<>(50000);

    void addBlacklist(String customerID, String freelancerID) {
        FastHashSet blacklisted = blacklistMap.get(customerID);
        if (blacklisted == null) {
            blacklisted = new FastHashSet(1024);
            blacklistMap.put(customerID, blacklisted);
        }
        blacklisted.add(freelancerID);
    }

    void removeBlacklist(String customerID, String freelancerID) {
        FastHashSet blacklisted = blacklistMap.get(customerID);
        if (blacklisted != null) {
            blacklisted.remove(freelancerID);
        }
    }

    boolean isBlacklisted(String customerID, String freelancerID) {
        FastHashSet blacklisted = blacklistMap.get(customerID);
        return blacklisted != null && blacklisted.contains(freelancerID);
    }

    int getBlacklistedCountForCustomer(String customerID) {
        FastHashSet blacklisted = blacklistMap.get(customerID);
        return blacklisted != null ? blacklisted.size() : 0;
    }

    public FastHashSet getBlacklistedSetForCustomer(String customerID) {
        return blacklistMap.get(customerID);
    }
}

/**
 * Simple container for a Freelancer and their computed score, used for sorting.
 */
class FreelancerScore {
    Freelancer freelancer;
    int score;
    String id;

    FreelancerScore() {}

    void set(Freelancer freelancer, int score) {
        this.freelancer = freelancer;
        this.score = score;
        this.id = freelancer.freelancerID;
    }
}

/**
 * Main class containing the simulation logic and command processing.
 */
public class Main {
    // Custom FastHashMap to replace standard Java collections for performance
    private static final FastHashMap<Customer> customers = new FastHashMap<>(50000);
    private static final FastHashMap<Freelancer> freelancers = new FastHashMap<>(100000);
    private static final FastHashMap<Employment> activeEmployments = new FastHashMap<>(10000);
    private static final Blacklists blacklists = new Blacklists();
    private static final FastHashMap<Services> serviceCache = new FastHashMap<>(32);
    private static final FastHashMap<ServiceGroup> freelancersByService = new FastHashMap<>(32);

    private static final StringBuilder SB = new StringBuilder(32768);
    private static final Freelancer[] MOVE_BUFFER = new Freelancer[100000];

    // Object pool for FreelancerScore to minimize garbage collection
    private static final int MAX_K = 1000;
    private static final FreelancerScore[] SCORE_POOL = new FreelancerScore[MAX_K];
    private static final FreelancerScore[] topKArray = new FreelancerScore[MAX_K];

    // Interface to allow Service creation lambda-style for FastHashMap.computeIfAbsent
    interface ServiceFactory {
        Services create(String key);
    }

    static final ServiceFactory SERVICE_FACTORY = Services::new;

    static {
        for (int i = 0; i < MAX_K; i++) {
            SCORE_POOL[i] = new FreelancerScore();
            topKArray[i] = new FreelancerScore();
        }
    }

    // Comparator for the Min-Heap: scores ascending, IDs descending (tiebreaker)
    private static final Comparator<FreelancerScore> MIN_HEAP_COMPARATOR = new Comparator<FreelancerScore>() {
        public int compare(FreelancerScore fs1, FreelancerScore fs2) {
            int scoreCompare = Integer.compare(fs1.score, fs2.score);
            if (scoreCompare != 0) return scoreCompare;
            return fs2.id.compareTo(fs1.id); // Descending ID
        }
    };

    // Comparator for the final output sort: scores descending, IDs ascending (tiebreaker)
    private static final Comparator<FreelancerScore> FINAL_SORT_COMPARATOR = new Comparator<FreelancerScore>() {
        public int compare(FreelancerScore fs1, FreelancerScore fs2) {
            int scoreCompare = Integer.compare(fs2.score, fs1.score); // Descending Score
            if (scoreCompare != 0) return scoreCompare;
            return fs1.id.compareTo(fs2.id); // Ascending ID
        }
    };

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java Main <input_file> <output_file>");
            System.exit(1);
        }

        FastReader reader = null;
        FastWriter writer = null;

        try {
            reader = new FastReader(new FileInputStream(args[0]));
            writer = new FastWriter(new FileOutputStream(args[1]));

            String token;
            while ((token = reader.nextToken()) != null) {
                processCommand(token, reader, writer);
            }

        } catch (IOException e) {
            System.err.println("Error reading/writing files: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (writer != null) writer.close();
            } catch (IOException e) { /* Silent close */ }
            try {
                if (reader != null) reader.close();
            } catch (IOException e) { /* Silent close */ }
        }
    }

    /**
     * Reads and delegates the processing of a single command.
     */
    private static void processCommand(String operation, FastReader reader, FastWriter writer) throws IOException {
        try {
            String result = "";
            String[] parts;

            switch (operation) {
                case "register_customer":
                    parts = new String[]{operation, reader.nextToken()};
                    result = registerCustomer(parts);
                    break;
                case "register_freelancer":
                    parts = new String[]{operation, reader.nextToken(), reader.nextToken(), reader.nextToken(), reader.nextToken(), reader.nextToken(), reader.nextToken(), reader.nextToken(), reader.nextToken()};
                    result = registerFreelancer(parts);
                    break;
                case "request_job":
                    parts = new String[]{operation, reader.nextToken(), reader.nextToken(), reader.nextToken()};
                    result = requestJob(parts);
                    break;
                case "employ_freelancer":
                    parts = new String[]{operation, reader.nextToken(), reader.nextToken()};
                    result = employFreelancer(parts);
                    break;
                case "complete_and_rate":
                    parts = new String[]{operation, reader.nextToken(), reader.nextToken()};
                    result = completeAndRate(parts);
                    break;
                case "cancel_by_freelancer":
                    parts = new String[]{operation, reader.nextToken()};
                    result = cancelByFreelancer(parts);
                    break;
                case "cancel_by_customer":
                    parts = new String[]{operation, reader.nextToken(), reader.nextToken()};
                    result = cancelByCustomer(parts);
                    break;
                case "blacklist":
                    parts = new String[]{operation, reader.nextToken(), reader.nextToken()};
                    result = blacklist(parts);
                    break;
                case "unblacklist":
                    parts = new String[]{operation, reader.nextToken(), reader.nextToken()};
                    result = unblacklist(parts);
                    break;
                case "change_service":
                    parts = new String[]{operation, reader.nextToken(), reader.nextToken(), reader.nextToken()};
                    result = changeService(parts);
                    break;
                case "simulate_month":
                    result = simulateMonth(null);
                    break;
                case "query_freelancer":
                    parts = new String[]{operation, reader.nextToken()};
                    result = queryFreelancer(parts);
                    break;
                case "query_customer":
                    parts = new String[]{operation, reader.nextToken()};
                    result = queryCustomer(parts);
                    break;
                case "update_skill":
                    parts = new String[]{operation, reader.nextToken(), reader.nextToken(), reader.nextToken(), reader.nextToken(), reader.nextToken(), reader.nextToken()};
                    result = updateSkill(parts);
                    break;
                default:
                    result = "Unknown command: " + operation;
            }

            writer.write(result);
            writer.newLine();

        } catch (Exception e) {
            // Silent error handling for malformed input
        }
    }

    /**
     * Registers a new customer.
     */
    private static String registerCustomer(String[] parts) {
        String customerID = parts[1];

        if (customers.containsKey(customerID) || freelancers.containsKey(customerID)) {
            return "Some error occurred in register_customer.";
        }

        Customer customer = new Customer(customerID);
        customers.put(customerID, customer);
        return "registered customer " + customerID;
    }

    /**
     * Registers a new freelancer and adds them to the appropriate ServiceGroup.
     */
    private static String registerFreelancer(String[] parts) {
        String freelancerID = parts[1];
        String serviceName = parts[2];
        double basePrice;
        int T, C, R, E, A;

        try {
            basePrice = Double.parseDouble(parts[3]);
            T = Integer.parseInt(parts[4]);
            C = Integer.parseInt(parts[5]);
            R = Integer.parseInt(parts[6]);
            E = Integer.parseInt(parts[7]);
            A = Integer.parseInt(parts[8]);
        } catch (Exception e) {
            return "Some error occurred in register_freelancer.";
        }


        if (customers.containsKey(freelancerID) || freelancers.containsKey(freelancerID)) {
            return "Some error occurred in register_freelancer.";
        }

        if (!Services.isValidServiceType(serviceName)) {
            return "Some error occurred in register_freelancer.";
        }

        if (basePrice <= 0) {
            return "Some error occurred in register_freelancer.";
        }

        // Validate skill ranges
        if (T < 0 || T > 100 || C < 0 || C > 100 || R < 0 || R > 100 ||
                E < 0 || E > 100 || A < 0 || A > 100) {
            return "Some error occurred in register_freelancer.";
        }

        Freelancer freelancer = new Freelancer(freelancerID, serviceName, basePrice, T, C, R, E, A);
        freelancers.put(freelancerID, freelancer);

        ServiceGroup serviceGroup = freelancersByService.get(serviceName);
        if (serviceGroup == null) {
            serviceGroup = new ServiceGroup();
            freelancersByService.put(serviceName, serviceGroup);
        }
        serviceGroup.add(freelancer);

        return "registered freelancer " + freelancerID;
    }

    /**
     * Finds the top K available, non-blacklisted freelancers for a service,
     * employs the best one, and returns the sorted list.
     */
    private static String requestJob(String[] parts) {
        String customerID = parts[1];
        String serviceName = parts[2];
        int topK;
        try {
            topK = Integer.parseInt(parts[3]);
        } catch (NumberFormatException e) {
            return "Some error occurred in request_job.";
        }

        // Capping topK to prevent array overflow and excessive computation
        if (topK > MAX_K) topK = MAX_K;


        Customer customer = customers.get(customerID);

        if (customer == null) {
            return "Some error occurred in request_job.";
        }

        if (!Services.isValidServiceType(serviceName)) {
            return "Some error occurred in request_job.";
        }

        ServiceGroup serviceGroup = freelancersByService.get(serviceName);

        if (serviceGroup == null || serviceGroup.availableSize == 0) {
            return "no freelancers available";
        }

        Services service = serviceCache.computeIfAbsent(serviceName, SERVICE_FACTORY);

        int heapSize = 0;
        int eligibleFreelancerCount = 0;

        // Use the O(1) availableList
        Freelancer[] list = serviceGroup.availableList;
        int size = serviceGroup.availableSize;

        FastHashSet blacklistedSet = blacklists.getBlacklistedSetForCustomer(customerID);

        // Iterate over available freelancers
        for (int i = 0; i < size; i++) {
            Freelancer freelancer = list[i];

            if (blacklistedSet != null && blacklistedSet.contains(freelancer.freelancerID)) {
                continue; // Skip blacklisted freelancers
            }

            // Eligible freelancer found
            eligibleFreelancerCount++;

            int compositeScore = freelancer.getOrComputeScore(service);

            // Maintain a Min-Heap of size topK
            if (heapSize < topK) {
                // Use the pre-allocated array slot
                topKArray[heapSize].set(freelancer, compositeScore);
                heapifyUp(topKArray, heapSize);
                heapSize++;
            } else {
                FreelancerScore minFs = topKArray[0];

                if (compositeScore > minFs.score) {
                    minFs.set(freelancer, compositeScore);
                    heapifyDown(topKArray, heapSize, 0);
                } else if (compositeScore == minFs.score) {
                    // Tiebreaker: prioritize smaller ID
                    if (freelancer.freelancerID.compareTo(minFs.id) < 0) {
                        minFs.set(freelancer, compositeScore);
                        heapifyDown(topKArray, heapSize, 0);
                    }
                }
            }
        }

        if (eligibleFreelancerCount == 0) { // Check if no eligible freelancer was found
            return "no freelancers available";
        }

        // Use a temporary ArrayList for sorting
        ArrayList<FreelancerScore> sortList = new ArrayList<>(heapSize);
        // Optimization: Use the object pool for sorting objects to reduce garbage collection load
        for(int i = 0; i < heapSize; i++) {
            SCORE_POOL[i].set(topKArray[i].freelancer, topKArray[i].score);
            sortList.add(SCORE_POOL[i]);
        }

        // Sort by score descending, then ID ascending (O(K log K) Quick Sort)
        ListSorter.sort(sortList, FINAL_SORT_COMPARATOR);

        SB.setLength(0);

        // Output the top list
        int displayCount = sortList.size();
        SB.append("available freelancers for ").append(serviceName).append(" (top ").append(displayCount).append("):");

        for (int i = 0; i < displayCount; i++) {
            FreelancerScore fs = sortList.get(i);
            SB.append("\n");
            SB.append(fs.id)
                    .append(" - composite: ").append(fs.score)
                    .append(", price: ").append(Math.round(fs.freelancer.basePrice))
                    .append(", rating: ");
            long r10 = Math.round(fs.freelancer.rating * 10.0);
            // Format rating to one decimal place
            SB.append(r10 / 10).append('.').append(Math.abs(r10 % 10));
        }

        // Auto-employ the best freelancer
        FreelancerScore best = sortList.get(0);
        Freelancer bestFreelancer = best.freelancer;

        bestFreelancer.available = false;
        serviceGroup.notifyUnavailable(bestFreelancer); // O(1) update

        Employment employment = new Employment(customer, bestFreelancer);
        activeEmployments.put(bestFreelancer.freelancerID, employment);

        customer.totalEmploymentCount++;

        SB.append("\n");
        SB.append("auto-employed best freelancer: ")
                .append(bestFreelancer.freelancerID)
                .append(" for customer ")
                .append(customerID);

        return SB.toString();
    }

    /**
     * Restores the Min-Heap property upwards.
     */
    private static void heapifyUp(FreelancerScore[] heap, int index) {
        while (index > 0) {
            int parent = (index - 1) >> 1;
            if (MIN_HEAP_COMPARATOR.compare(heap[index], heap[parent]) < 0) {
                FreelancerScore temp = heap[index];
                heap[index] = heap[parent];
                heap[parent] = temp;
                index = parent;
            } else {
                break;
            }
        }
    }

    /**
     * Restores the Min-Heap property downwards.
     */
    private static void heapifyDown(FreelancerScore[] heap, int heapSize, int index) {
        while (true) {
            int smallest = index;
            int left = (index << 1) + 1;
            int right = left + 1;

            if (left < heapSize && MIN_HEAP_COMPARATOR.compare(heap[left], heap[smallest]) < 0) {
                smallest = left;
            }
            if (right < heapSize && MIN_HEAP_COMPARATOR.compare(heap[right], heap[smallest]) < 0) {
                smallest = right;
            }

            if (smallest != index) {
                FreelancerScore temp = heap[index];
                heap[index] = heap[smallest];
                heap[smallest] = temp;
                index = smallest;
            } else {
                break;
            }
        }
    }

    /**
     * Manually employs an available freelancer for a customer.
     */
    private static String employFreelancer(String[] parts) {
        String customerID = parts[1];
        String freelancerID = parts[2];

        Customer customer = customers.get(customerID);
        if (customer == null) {
            return "Some error occurred in employ_freelancer.";
        }

        Freelancer freelancer = freelancers.get(freelancerID);
        if (freelancer == null) {
            return "Some error occurred in employ_freelancer.";
        }

        if (!freelancer.available) {
            return "Some error occurred in employ_freelancer.";
        }

        if (blacklists.isBlacklisted(customerID, freelancerID)) {
            return "Some error occurred in employ_freelancer.";
        }

        // O(1) unavailable notification
        ServiceGroup sg = freelancersByService.get(freelancer.serviceName);
        if (sg != null) sg.notifyUnavailable(freelancer);

        freelancer.available = false;

        Employment employment = new Employment(customer, freelancer);
        activeEmployments.put(freelancerID, employment);

        customer.totalEmploymentCount++;

        return customerID + " employed " + freelancerID + " for " + freelancer.serviceName;
    }

    /**
     * Completes a job, updates ratings, skills, and loyalty. (MANTIK DÜZELTİLDİ)
     */
    private static String completeAndRate(String[] parts) {
        if (parts.length != 3) {
            return "Some error occurred in complete_and_rate.";
        }

        String freelancerID = parts[1];
        int rating;

        try {
            rating = Integer.parseInt(parts[2]);
        } catch (NumberFormatException e) {
            return "Some error occurred in complete_and_rate.";
        }

        if (rating < 0 || rating > 5) {
            return "Some error occurred in complete_and_rate.";
        }

        Freelancer freelancer = freelancers.get(freelancerID);
        if (freelancer == null) {
            return "Some error occurred in complete_and_rate.";
        }

        Employment employment = activeEmployments.get(freelancerID);
        if (employment == null) {
            return "Some error occurred in complete_and_rate.";
        }

        Customer customer = employment.customer;
        String customerID = customer.customerID;
        double basePrice = freelancer.basePrice;

        String currentTier = customer.loyaltyTier;
        double discountFactor = 1.0;

        if (currentTier.equals("PLATINUM")) {
            discountFactor = 0.85;
        } else if (currentTier.equals("GOLD")) {
            discountFactor = 0.90;
        } else if (currentTier.equals("SILVER")) {
            discountFactor = 0.95;
        }

        double customerPayment = Math.floor(basePrice * discountFactor);
        customer.totalAmountSpent += customerPayment;
        customer.totalLoyaltyBase += customerPayment;

        if (rating >= 4) {
            Services service = serviceCache.computeIfAbsent(freelancer.serviceName, SERVICE_FACTORY);
            freelancer.gainSkillsFromJob(service, rating);
        }

        // --- MANTIK DÜZELTME: Derecelendirme Formülü (Orijinal Kod İle Aynı) ---
        double currentRating = freelancer.rating;
        // n'in tanımı: completedJobs + cancelledJobs + 1 (Bu TAMAMLANMADAN ÖNCEKİ toplam iş + 1)
        int n = freelancer.completedJobs + freelancer.cancelledJobs + 1;

        // Formül: (currentRating * n + rating) / (n + 1.0)
        double newRating = ((currentRating * n) + rating) / (n + 1.0);
        freelancer.rating = newRating;

        freelancer.completedJobs++;
        freelancer.monthlyCompletedJobs++;
        freelancer.invalidateScore();
        // ---------------------------------------------

        ServiceGroup sg = freelancersByService.get(freelancer.serviceName);
        if (sg != null) sg.notifyAvailable(freelancer);
        freelancer.available = true;

        activeEmployments.remove(freelancerID);

        return freelancerID + " completed job for " + customerID + " with rating " + rating;
    }

    /**
     * A freelancer cancels an active job, incurring penalties. (MANTIK DÜZELTİLDİ)
     */
    private static String cancelByFreelancer(String[] parts) {
        String freelancerID = parts[1];

        Freelancer freelancer = freelancers.get(freelancerID);
        if (freelancer == null) {
            return "Some error occurred in cancel_by_freelancer.";
        }

        Employment employment = activeEmployments.get(freelancerID);
        if (employment == null) {
            return "Some error occurred in cancel_by_freelancer.";
        }

        String customerID = employment.customer.customerID;

        freelancer.applySkillDegradation();
        freelancer.cancelledJobs++; // İptal sayısı artırıldı
        freelancer.monthlyCancellations++;

        // --- MANTIK DÜZELTME: Derecelendirme Formülü (Orijinal Kod İle Aynı) ---
        double currentRating = freelancer.rating;
        int completedJobs = freelancer.completedJobs;
        int cancelledJobs = freelancer.cancelledJobs; // Artırılmış değer

        // Orijinal formülün n tanımı: completedJobs + cancelledJobs + 1 (Bu İPTAL SONRASI toplam iş + 1)
        int n = completedJobs + cancelledJobs + 1;

        // Formül: (currentRating * (n - 1)) / n
        double newRating = (currentRating * (n - 1)) / (double) n;
        newRating = Math.max(0.0, newRating);
        freelancer.rating = newRating;

        freelancer.invalidateScore();
        // ---------------------------------------------

        activeEmployments.remove(freelancerID);

        String baseOutput = "cancelled by freelancer: " + freelancerID + " cancelled " + customerID;

        if (freelancer.monthlyCancellations >= 5) {
            ServiceGroup serviceGroup = freelancersByService.get(freelancer.serviceName);
            if (serviceGroup != null) {
                serviceGroup.remove(freelancer);
            }

            freelancers.remove(freelancerID);
            return baseOutput + "\n" + "platform banned freelancer: " + freelancerID;
        }

        ServiceGroup sg = freelancersByService.get(freelancer.serviceName);
        if (sg != null) sg.notifyAvailable(freelancer);
        freelancer.available = true;

        return baseOutput;
    }

    /**
     * A customer cancels an active job, leading to a cancellation count increase.
     */
    private static String cancelByCustomer(String[] parts) {
        String customerID = parts[1];
        String freelancerID = parts[2];

        Customer customer = customers.get(customerID);
        if (customer == null) {
            return "Some error occurred in cancel_by_customer.";
        }

        Freelancer freelancer = freelancers.get(freelancerID);
        if (freelancer == null) {
            return "Some error occurred in cancel_by_customer.";
        }

        Employment employment = activeEmployments.get(freelancerID);
        if (employment == null || !employment.customer.customerID.equals(customerID)) {
            return "Some error occurred in cancel_by_customer.";
        }

        customer.cancellationCount++;

        // O(1) available notification
        ServiceGroup sg = freelancersByService.get(freelancer.serviceName);
        if (sg != null) sg.notifyAvailable(freelancer);
        freelancer.available = true;

        activeEmployments.remove(freelancerID);

        return "cancelled by customer: " + customerID + " cancelled " + freelancerID;
    }

    /**
     * Adds a freelancer to a customer's blacklist.
     */
    private static String blacklist(String[] parts) {
        if (parts.length != 3) {
            return "Some error occurred in blacklist.";
        }

        String customerID = parts[1];
        String freelancerID = parts[2];

        if (!customers.containsKey(customerID)) {
            return "Some error occurred in blacklist.";
        }

        if (!freelancers.containsKey(freelancerID)) {
            return "Some error occurred in blacklist.";
        }

        if (blacklists.isBlacklisted(customerID, freelancerID)) {
            return "Some error occurred in blacklist.";
        }

        blacklists.addBlacklist(customerID, freelancerID);
        return customerID + " blacklisted " + freelancerID;
    }

    /**
     * Removes a freelancer from a customer's blacklist.
     */
    private static String unblacklist(String[] parts) {
        if (parts.length != 3) {
            return "Some error occurred in unblacklist.";
        }

        String customerID = parts[1];
        String freelancerID = parts[2];

        if (!customers.containsKey(customerID)) {
            return "Some error occurred in unblacklist.";
        }

        if (!freelancers.containsKey(freelancerID)) {
            return "Some error occurred in unblacklist.";
        }

        if (!blacklists.isBlacklisted(customerID, freelancerID)) {
            return "Some error occurred in unblacklist.";
        }

        blacklists.removeBlacklist(customerID, freelancerID);
        return customerID + " unblacklisted " + freelancerID;
    }

    /**
     * Queues a service change for a freelancer.
     */
    private static String changeService(String[] parts) {
        if (parts.length != 4) {
            return "Some error occurred in change_service.";
        }

        String freelancerID = parts[1];
        String newService = parts[2].toLowerCase();
        double newPrice;

        try {
            newPrice = Double.parseDouble(parts[3]);
        } catch (NumberFormatException e) {
            return "Some error occurred in change_service.";
        }

        Freelancer freelancer = freelancers.get(freelancerID);
        if (freelancer == null) {
            return "Some error occurred in change_service.";
        }

        if (!Services.isValidServiceType(newService)) {
            return "Some error occurred in change_service.";
        }

        if (newPrice <= 0) {
            return "Some error occurred in change_service.";
        }

        if (!freelancer.available) {
            return "Some error occurred in change_service.";
        }

        if (freelancer.serviceName.equals(newService)) {
            return "Some error occurred in change_service.";
        }

        String oldService = freelancer.serviceName;
        freelancer.queuedService = newService;
        freelancer.queuedPrice = newPrice;

        return "service change for " + freelancerID + " queued from " + oldService + " to " + newService;
    }

    /**
     * Simulates the end of a month: updates burnout, executes queued service changes, and updates loyalty tiers.
     */
    private static String simulateMonth(String[] parts) {
        // Collect all freelancers (O(N) for iteration)
        ArrayList<Freelancer> allFreelancers = freelancers.valuesList();
        int totalFreelancerCount = allFreelancers.size();

        int moveCount = 0;

        // Step 1: Update freelancer burnout status and prepare for service change
        for (int i = 0; i < totalFreelancerCount; i++) {
            Freelancer freelancer = allFreelancers.get(i);
            int monthlyCompleted = freelancer.monthlyCompletedJobs;

            // Update burnout status
            if (!freelancer.burnout && monthlyCompleted >= 5) {
                freelancer.burnout = true;
                freelancer.invalidateScore();
            } else if (freelancer.burnout && monthlyCompleted <= 2) {
                freelancer.burnout = false;
                freelancer.invalidateScore();
            }

            freelancer.monthlyCompletedJobs = 0;

            // Collect freelancers with a queued service change
            if (freelancer.queuedService != null) {
                if (moveCount < MOVE_BUFFER.length) {
                    MOVE_BUFFER[moveCount++] = freelancer;
                }
            }

            freelancer.monthlyCancellations = 0;
        }

        // Step 2: Remove freelancers from their old ServiceGroups
        for (int i = 0; i < moveCount; i++) {
            Freelancer freelancer = MOVE_BUFFER[i];
            ServiceGroup oldServiceGroup = freelancersByService.get(freelancer.serviceName);
            if (oldServiceGroup != null) {
                oldServiceGroup.remove(freelancer); // Removes from both main and available lists in O(1)
            }
        }

        // Step 3: Apply service change and add freelancers to their new ServiceGroups
        for (int i = 0; i < moveCount; i++) {
            Freelancer freelancer = MOVE_BUFFER[i];
            String newService = freelancer.queuedService;
            double newPrice = freelancer.queuedPrice;

            freelancer.serviceName = newService;
            freelancer.basePrice = newPrice;
            freelancer.queuedService = null;
            freelancer.queuedPrice = 0;
            freelancer.invalidateScore();

            // Add to the new ServiceGroup (which also calls notifyAvailable since freelancer.available is true)
            ServiceGroup serviceGroup = freelancersByService.get(newService);
            if (serviceGroup == null) {
                serviceGroup = new ServiceGroup();
                freelancersByService.put(newService, serviceGroup);
            }
            serviceGroup.add(freelancer);
            MOVE_BUFFER[i] = null; // Clean up buffer for GC
        }

        // Step 4: Update customer loyalty tiers
        ArrayList<Customer> allCustomers = customers.valuesList();

        for (int i = 0; i < allCustomers.size(); i++) {
            allCustomers.get(i).updateLoyaltyTier();
        }

        return "month complete";
    }

    /**
     * Retrieves and formats information about a specific freelancer.
     */
    private static String queryFreelancer(String[] parts) {
        if (parts.length != 2) {
            return "Some error occurred in query_freelancer.";
        }

        String freelancerID = parts[1];
        Freelancer freelancer = freelancers.get(freelancerID);
        if (freelancer == null) {
            return "Some error occurred in query_freelancer.";
        }

        SB.setLength(0);
        SB.append(freelancerID).append(": ")
                .append(freelancer.serviceName)
                .append(", price: ").append(Math.round(freelancer.basePrice))
                .append(", rating: ");
        long r10 = Math.round(freelancer.rating * 10.0);
        // Format rating to one decimal place
        SB.append(r10 / 10).append('.').append(Math.abs(r10 % 10));
        SB.append(", completed: ").append(freelancer.completedJobs)
                .append(", cancelled: ").append(freelancer.cancelledJobs)
                .append(", skills: (")
                .append(freelancer.technicalProficiency).append(',')
                .append(freelancer.communication).append(',')
                .append(freelancer.creativity).append(',')
                .append(freelancer.efficiency).append(',')
                .append(freelancer.attentionToDetail).append("), available: ")
                .append(freelancer.available ? "yes" : "no")
                .append(", burnout: ").append(freelancer.burnout ? "yes" : "no");

        return SB.toString();
    }

    /**
     * Retrieves and formats information about a specific customer.
     */
    private static String queryCustomer(String[] parts) {
        if (parts.length != 2) {
            return "Some error occurred in query_customer.";
        }

        String customerID = parts[1];
        Customer customer = customers.get(customerID);
        if (customer == null) {
            return "Some error occurred in query_customer.";
        }

        int totalSpent = (int) Math.round(customer.totalAmountSpent);
        String loyaltyTier = customer.loyaltyTier;
        int blacklistedCount = blacklists.getBlacklistedCountForCustomer(customerID);
        int totalEmploymentCount = customer.totalEmploymentCount;

        SB.setLength(0);
        SB.append(customerID).append(": total spent: $").append(totalSpent)
                .append(", loyalty tier: ").append(loyaltyTier)
                .append(", blacklisted freelancer count: ").append(blacklistedCount)
                .append(", total employment count: ").append(totalEmploymentCount);

        return SB.toString();
    }

    /**
     * Updates the skill scores of a freelancer.
     */
    private static String updateSkill(String[] parts) {
        if (parts.length != 7) {
            return "Some error occurred in update_skill.";
        }

        String freelancerID = parts[1];

        Freelancer freelancer = freelancers.get(freelancerID);
        if (freelancer == null) {
            return "Some error occurred in update_skill.";
        }

        int T, C, R, E, A;
        try {
            T = Integer.parseInt(parts[2]);
            C = Integer.parseInt(parts[3]);
            R = Integer.parseInt(parts[4]);
            E = Integer.parseInt(parts[5]);
            A = Integer.parseInt(parts[6]);
        } catch (NumberFormatException e) {
            return "Some error occurred in update_skill.";
        }

        // Validate skill scores range
        if (T < 0 || T > 100 || C < 0 || C > 100 || R < 0 || R > 100 ||
                E < 0 || E > 100 || A < 0 || A > 100) {
            return "Some error occurred in update_skill.";
        }

        // Apply new skill scores, constrained between 0 and 100
        freelancer.technicalProficiency = Math.min(100, Math.max(0, T));
        freelancer.communication = Math.min(100, Math.max(0, C));
        freelancer.creativity = Math.min(100, Math.max(0, R));
        freelancer.efficiency = Math.min(100, Math.max(0, E));
        freelancer.attentionToDetail = Math.min(100, Math.max(0, A));
        freelancer.invalidateScore();

        String serviceName = freelancer.serviceName;

        return "updated skills of " + freelancerID + " for " + serviceName;
    }
}