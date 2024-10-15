namespace puvsWordCountComparison;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Diagnostics;
using System.Threading;

public class Program
{
    // Pfad zur Textdatei
    private const string FILE_PATH = "/Users/bernhdt/KingJamesBible20.txt";
    private const int FILE_MULTIPLICATION_FACTOR = 3;
    private const int DELAY = 0;

    // Anzahl der Wiederholungen
    const int ITERATIONS = 1;
    const int THREAD_COUNT = 4;

    private static void Main(string[] args)
    {
        // Sequentielle Verarbeitung
        Console.WriteLine("Sequentielle Verarbeitung:");
        long sequentialTime = RunWithTotalTimeMeasurement(ITERATIONS, ProcessingMethod.Sequential);

        // Parallele Verarbeitung mit gemeinsamen Datenstrukturen für Threads
        Console.WriteLine("\nParallele Verarbeitung mit Threads (gemeinsame Datenstrukturen):");
        long parallelTimeCommonData = RunWithTotalTimeMeasurement(ITERATIONS, ProcessingMethod.ParallelCommonData);

        // Parallele Verarbeitung mit isolierten Datenstrukturen für Threads
        Console.WriteLine("\nParallele Verarbeitung mit Threads (isolierte Datenstrukturen):");
        long parallelTimeSeparateData = RunWithTotalTimeMeasurement(ITERATIONS, ProcessingMethod.ParallelSeparatedDate);
        
        // Sequentielle Verarbeitung
        Console.WriteLine("Sequentielle Verarbeitung (Levenshtein):");
        long sequentialTimeLevenshtein = RunWithTotalTimeMeasurement(ITERATIONS, ProcessingMethod.SequentialLevenshtein);

        // Parallele Verarbeitung mit gemeinsamen Datenstrukturen für Threads
        Console.WriteLine("\nParallele Verarbeitung (Levenshtein) mit Threads (gemeinsame Datenstrukturen):");
        long parallelTimeLevenshteinCommonData = RunWithTotalTimeMeasurement(ITERATIONS, ProcessingMethod.ParallelLevenshtein);

        // Ausgabe der Vergleichsergebnisse
        Console.WriteLine($"\n\nSequentielle Gesamtzeit: {sequentialTime} ms");
        Console.WriteLine($"Durchschnittliche Zeit für {ITERATIONS} Iterationen: {sequentialTime / (double) ITERATIONS} ms");

        Console.WriteLine($"\nParallele Gesamtzeit mit gemeinsamen Daten: {parallelTimeCommonData} ms");
        Console.WriteLine($"Durchschnittliche Zeit für {ITERATIONS} Iterationen: {parallelTimeCommonData / (double) ITERATIONS} ms");

        Console.WriteLine($"\nParallele Gesamtzeit mit getrennten Daten: {parallelTimeSeparateData} ms");
        Console.WriteLine($"Durchschnittliche Zeit für {ITERATIONS} Iterationen: {parallelTimeSeparateData / (double) ITERATIONS} ms");

        Console.WriteLine($"\nSequentielle Gesamtzeit (Levenshtein): {sequentialTimeLevenshtein} ms");
        Console.WriteLine($"Durchschnittliche Zeit für {ITERATIONS} Iterationen: {sequentialTimeLevenshtein / (double) ITERATIONS} ms");

        Console.WriteLine($"\nParallele Gesamtzeit mit gemeinsamen Daten (Levenshtein): {parallelTimeLevenshteinCommonData} ms");
        Console.WriteLine($"Durchschnittliche Zeit für {ITERATIONS} Iterationen: {parallelTimeLevenshteinCommonData / (double) ITERATIONS} ms");
    }

    // Methode zur Ausführung mit Zeitmessung für alle Durchläufe
    private static long RunWithTotalTimeMeasurement(int iterations, ProcessingMethod method)
    {
        long totalElapsedTime = 0;

        for (int i = 1; i <= iterations; i++)
        {
            long elapsedTime = ProcessAndTimeMeasurement(i, method);
            totalElapsedTime += elapsedTime;
        }

        return totalElapsedTime;
    }
    
    // Methode zur Verarbeitung der Daten und Zeitmessung pro Durchlauf
    private static long ProcessAndTimeMeasurement(int iteration, ProcessingMethod processingMethod)
    {
        string methodInfo;
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.Start();

        switch (processingMethod)
        {
            case ProcessingMethod.Sequential:
                // Sequentielle Auswertung
                ProcessFileSequential(iteration);
                methodInfo = "(sequentiell)";
                break;
            case ProcessingMethod.ParallelCommonData:
                // Parallele Auswertung mit Threads und geteilten Daten
                ProcessFileParallelWithThreadsAndCommonData(iteration);
                methodInfo = "(parallel, common data)";
                break;
            case ProcessingMethod.ParallelSeparatedDate:
                // Parallele Auswertung mit Threads und separate Daten
                ProcessFileParallelWithThreadsAndSeparateData(iteration);
                methodInfo = "(parallel, separate data)";
                break;
            case ProcessingMethod.SequentialLevenshtein:
                // Parallele Auswertung mit Threads und separate Daten
                ProcessLevenshteinSequential(iteration);
                methodInfo = "(Levenshtein, sequential)";
                break;
            case ProcessingMethod.ParallelLevenshtein:
                // Parallele Auswertung mit Threads und separate Daten
                ProcessLevenshteinParallelWithThreadsAndCommonData(iteration);
                methodInfo = "(Levenshtein, parallel, common data)";
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(processingMethod), processingMethod, null);
        }

        stopwatch.Stop();
        Console.WriteLine($"Zeit für Iteration {iteration} {methodInfo}: {stopwatch.ElapsedMilliseconds} ms\n");
        return stopwatch.ElapsedMilliseconds;
    }

    // Methode zur parallelen Verarbeitung der Datei mit isolierten Datenstrukturen in Threads
    private static void ProcessFileParallelWithThreadsAndSeparateData(int iteration)
    {
        // Textdatei einlesen
        string text = ReadFile(FILE_PATH, FILE_MULTIPLICATION_FACTOR); // File.ReadAllText(filePath);

        // Wörter extrahieren, in Kleinbuchstaben umwandeln und nach Leerzeichen trennen
        string[] words = text
            .ToLower()
            .Split(new char[] {' ', '\r', '\n', '\t', ',', '.', ';', ':', '-', '_', '!', '?'},
                StringSplitOptions.RemoveEmptyEntries);

        // Anzahl der Threads und Aufteilung der Arbeit
        int wordsPerThread = words.Length / THREAD_COUNT;
        Thread[] threads = new Thread[THREAD_COUNT];

        // Liste, um die Ergebnisse jedes Threads zu speichern
        List<Dictionary<string, int>> threadWordFrequencies = new List<Dictionary<string, int>>(THREAD_COUNT);

        // Initialisieren der Wörterbücher für jeden Thread
        for (int t = 0; t < THREAD_COUNT; t++)
        {
            threadWordFrequencies.Add(new Dictionary<string, int>());
        }

        for (int t = 0; t < THREAD_COUNT; t++)
        {
            int start = t * wordsPerThread;
            int end = (t == THREAD_COUNT - 1) ? words.Length : (t + 1) * wordsPerThread;

            // Jeder Thread verarbeitet einen Teil der Wörter in einem eigenen Wörterbuch
            int threadIndex = t; // Damit die Lambda-Funktion das richtige Wörterbuch verwendet
            threads[t] = new Thread(() =>
            {
                Dictionary<string, int> localWordFrequency = threadWordFrequencies[threadIndex];

                for (int i = start; i < end; i++)
                {
                    string word = words[i];
                    if (!localWordFrequency.TryAdd(word, 1))
                    {
                        localWordFrequency[word]++;
                    }

                    Thread.Sleep(DELAY);
                }
            });

            threads[t].Start();
        }

        // Warten auf alle Threads
        foreach (var thread in threads)
        {
            thread.Join();
        }

        // Zusammenführen der Ergebnisse aus allen Threads
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.Start();
        Dictionary<string, int> finalWordFrequency = new Dictionary<string, int>();
        foreach (var threadWordFrequency in threadWordFrequencies)
        {
            foreach (var kvp in threadWordFrequency)
            {
                if (finalWordFrequency.ContainsKey(kvp.Key))
                {
                    finalWordFrequency[kvp.Key] += kvp.Value;
                }
                else
                {
                    finalWordFrequency[kvp.Key] = kvp.Value;
                }
            }
        }

        long totalElapsedTime = stopwatch.ElapsedMilliseconds;
        Console.WriteLine($"Zeit zum Zusammenführen: {totalElapsedTime} ms");

        // Sortierung nach Häufigkeit und Auswahl der 10 häufigsten Wörter
        IEnumerable<KeyValuePair<string, int>> topWords = finalWordFrequency
            .OrderByDescending(w => w.Value)
            .Take(10);

        // Ausgabe der Ergebnisse für die aktuelle Iteration
        Console.WriteLine($"\nIteration {iteration} (parallel mit isolierten Threads):");
        Console.WriteLine("Die 10 häufigsten Wörter und deren Vorkommen:");
        foreach (var entry in topWords)
        {
            Console.WriteLine($"{entry.Key}: {entry.Value}");
        }
    }

    // Methode zur parallelen Verarbeitung der Datei mit Threads
    private static void ProcessFileParallelWithThreadsAndCommonData(int iteration)
    {
        // Textdatei einlesen
        string text = ReadFile(FILE_PATH, FILE_MULTIPLICATION_FACTOR); // File.ReadAllText(filePath);

        // Wörter extrahieren, in Kleinbuchstaben umwandeln und nach Leerzeichen trennen
        var words = text
            .ToLower()
            .Split(new char[] {' ', '\r', '\n', '\t', ',', '.', ';', ':', '-', '_', '!', '?'},
                StringSplitOptions.RemoveEmptyEntries);

        // Wörterbuch zur Zählung der Häufigkeit
        Dictionary<string, int> wordFrequency = new Dictionary<string, int>();
        object lockObject = new object(); // Lock-Objekt für den Zugriff auf das Wörterbuch

        // Anzahl der Threads und Aufteilung der Arbeit
        int wordsPerThread = words.Length / THREAD_COUNT;
        Thread[] threads = new Thread[THREAD_COUNT];

        for (int t = 0; t < THREAD_COUNT; t++)
        {
            int start = t * wordsPerThread;
            int end = (t == THREAD_COUNT - 1) ? words.Length : (t + 1) * wordsPerThread;

            // Jeder Thread verarbeitet einen Teil der Wörter
            threads[t] = new Thread(() =>
            {
                //Dictionary<string, int> localWordFrequency = new Dictionary<string, int>();

                for (int i = start; i < end; i++)
                {
                    string word = words[i];

                    lock (lockObject)
                    {
                        if (!wordFrequency.TryAdd(word, 1))
                        {
                            wordFrequency[word] += 1;
                        }
                    }

                    /*
                    if (!localWordFrequency.TryAdd(word, 1))
                    {
                        localWordFrequency[word]++;
                    }
                    */
                    Thread.Sleep(DELAY);
                }

                /*
                // Merge der lokalen Ergebnisse in das globale Wörterbuch
                lock (lockObject)
                {
                    foreach (var kvp in localWordFrequency)
                    {
                        if (wordFrequency.ContainsKey(kvp.Key))
                        {
                            wordFrequency[kvp.Key] += kvp.Value;
                        }
                        else
                        {
                            wordFrequency[kvp.Key] = kvp.Value;
                        }
                    }
                }
                */
            });

            threads[t].Start();
        }

        // Warten auf alle Threads
        foreach (var thread in threads)
        {
            thread.Join();
        }

        // Sortierung nach Häufigkeit und Auswahl der 10 häufigsten Wörter
        var topWords = wordFrequency
            .OrderByDescending(w => w.Value)
            .Take(10);

        // Ausgabe der Ergebnisse für die aktuelle Iteration
        Console.WriteLine($"\nIteration {iteration} (parallel mit Threads):");
        Console.WriteLine("Die 10 häufigsten Wörter und deren Vorkommen:");
        foreach (var entry in topWords)
        {
            Console.WriteLine($"{entry.Key}: {entry.Value}");
        }
    }

    // Methode zur sequentiellen Verarbeitung der Datei
    private static void ProcessFileSequential(int iteration)
    {
        // Textdatei einlesen
        string text = ReadFile(FILE_PATH, FILE_MULTIPLICATION_FACTOR); // File.ReadAllText(filePath);

        // Wörter extrahieren, in Kleinbuchstaben umwandeln und nach Leerzeichen trennen
        var words = text
            .ToLower()
            .Split(new char[] {' ', '\r', '\n', '\t', ',', '.', ';', ':', '-', '_', '!', '?'},
                StringSplitOptions.RemoveEmptyEntries);

        // Wörter zählen
        Dictionary<string, int> wordFrequency = new Dictionary<string, int>();

        foreach (var word in words)
        {
            if (!wordFrequency.TryAdd(word, 1))
            {
                wordFrequency[word]++;
            }

            Thread.Sleep(DELAY);
        }

        // Sortierung nach Häufigkeit und Auswahl der 10 häufigsten Wörter
        var topWords = wordFrequency
            .OrderByDescending(w => w.Value)
            .Take(10);

        // Ausgabe der Ergebnisse für die aktuelle Iteration
        Console.WriteLine($"\nIteration {iteration} (sequentiell):");
        Console.WriteLine("Die 10 häufigsten Wörter und deren Vorkommen:");
        foreach (var entry in topWords)
        {
            Console.WriteLine($"{entry.Key}: {entry.Value}");
        }
    }
    
      // Methode zur parallelen Verarbeitung der Datei mit Threads
    private static void ProcessLevenshteinParallelWithThreadsAndCommonData(int iteration)
    {
        // Textdatei einlesen
        string text = ReadFile(FILE_PATH, FILE_MULTIPLICATION_FACTOR); // File.ReadAllText(filePath);

        // Wörter extrahieren, in Kleinbuchstaben umwandeln und nach Leerzeichen trennen
        var words = text
            .ToLower()
            .Split(new char[] {' ', '\r', '\n', '\t', ',', '.', ';', ':', '-', '_', '!', '?'},
                StringSplitOptions.RemoveEmptyEntries);
        
        string sourceWord = words[0];

        // Wörterbuch zur Zählung der Häufigkeit
        Dictionary<string, int> wordDistance = new Dictionary<string, int>();
        object lockObject = new object(); // Lock-Objekt für den Zugriff auf das Wörterbuch

        // Anzahl der Threads und Aufteilung der Arbeit
        int wordsPerThread = words.Length / THREAD_COUNT;
        Thread[] threads = new Thread[THREAD_COUNT];

        for (int t = 0; t < THREAD_COUNT; t++)
        {
            int start = t * wordsPerThread;
            int end = (t == THREAD_COUNT - 1) ? words.Length : (t + 1) * wordsPerThread;

            // Jeder Thread verarbeitet einen Teil der Wörter
            threads[t] = new Thread(() =>
            {
                for (int i = start; i < end; i++)
                {
                    string word = words[i];
                    int distance = LevenshteinDistance(sourceWord, word);

                    lock (lockObject)
                    {
                        if (!wordDistance.TryAdd(word, distance))
                        {
                            wordDistance[word] += distance;
                        }
                    }
                    
                    Thread.Sleep(DELAY);
                }
            });

            threads[t].Start();
        }

        // Warten auf alle Threads
        foreach (var thread in threads)
        {
            thread.Join();
        }

        // Sortierung nach Häufigkeit und Auswahl der 10 häufigsten Wörter
        var topWords = wordDistance
            .OrderByDescending(w => w.Value)
            .Take(10);

        // Ausgabe der Ergebnisse für die aktuelle Iteration
        Console.WriteLine($"\nIteration {iteration} (parallel mit Threads):");
        Console.WriteLine("Die 10 häufigsten Wörter und deren Vorkommen:");
        foreach (var entry in topWords)
        {
            Console.WriteLine($"{entry.Key}: {entry.Value}");
        }
    }

    // Methode zur sequentiellen Verarbeitung der Datei
    private static void ProcessLevenshteinSequential(int iteration)
    {
        // Textdatei einlesen
        string text = ReadFile(FILE_PATH, FILE_MULTIPLICATION_FACTOR); // File.ReadAllText(filePath);

        // Wörter extrahieren, in Kleinbuchstaben umwandeln und nach Leerzeichen trennen
        var words = text
            .ToLower()
            .Split(new char[] {' ', '\r', '\n', '\t', ',', '.', ';', ':', '-', '_', '!', '?'},
                StringSplitOptions.RemoveEmptyEntries);
        
        string sourceWord = words[0];

        // Wörter zählen
        Dictionary<string, int> wordDistance = new Dictionary<string, int>();

        foreach (var word in words)
        {
            int distance = LevenshteinDistance(sourceWord, word);
            if (!wordDistance.TryAdd(word, distance))
            {
                wordDistance[word] += distance;
            }

            Thread.Sleep(DELAY);
        }

        // Sortierung nach Häufigkeit und Auswahl der 10 häufigsten Wörter
        var topWords = wordDistance
            .OrderByDescending(w => w.Value)
            .Take(10);

        // Ausgabe der Ergebnisse für die aktuelle Iteration
        Console.WriteLine($"\nIteration {iteration} (sequentiell):");
        Console.WriteLine("Die 10 häufigsten Wörter und deren Vorkommen:");
        foreach (var entry in topWords)
        {
            Console.WriteLine($"{entry.Key}: {entry.Value}");
        }
    }

    // Methode zum Einlesen der Textdatei
    private static string ReadFile(string filePath, int factor = 1)
    {
        string text = File.ReadAllText(filePath);

        for (int i = 1; i < factor; i++)
        {
            text += $" {text}";
        }

        return text;
    }
    
    // Methode zur Berechnung der Levenshtein-Distanz
    private static int LevenshteinDistance(string source, string target)
    {
        if (string.IsNullOrEmpty(source))
        {
            return target.Length;
        }

        if (string.IsNullOrEmpty(target))
        {
            return source.Length;
        }

        // Länge der Zeichenfolgen bestimmen
        int sourceLength = source.Length;
        int targetLength = target.Length;

        // Matrix zur Berechnung erstellen
        int[,] distance = new int[sourceLength + 1, targetLength + 1];

        // Basisfall: Leere Zeichenfolgen vergleichen
        for (int i = 0; i <= sourceLength; i++)
        {
            distance[i, 0] = i;
        }

        for (int j = 0; j <= targetLength; j++)
        {
            distance[0, j] = j;
        }

        // Berechnung der Levenshtein-Distanz
        for (int i = 1; i <= sourceLength; i++)
        {
            for (int j = 1; j <= targetLength; j++)
            {
                int cost = (source[i - 1] == target[j - 1]) ? 0 : 1;

                distance[i, j] = Math.Min(
                    Math.Min(distance[i - 1, j] + 1, // Entfernung eines Zeichens
                        distance[i, j - 1] + 1), // Einfügen eines Zeichens
                    distance[i - 1, j - 1] + cost); // Ersetzen eines Zeichens
            }
        }

        // Die Levenshtein-Distanz ist der Wert in der unteren rechten Ecke der Matrix
        return distance[sourceLength, targetLength];
    }
}

public enum ProcessingMethod
{
    Sequential,
    ParallelCommonData,
    ParallelSeparatedDate,
    SequentialLevenshtein,
    ParallelLevenshtein
}
