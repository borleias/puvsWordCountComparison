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
    private const int FACTOR = 3;

    private static void Main(string[] args)
    {
        // Anzahl der Wiederholungen
        const int iterations = 10;

        // Sequentielle Verarbeitung
        Console.WriteLine("Sequentielle Verarbeitung:");
        long sequentialTime = RunSequential(iterations);

        // Parallele Verarbeitung mit isolierten Datenstrukturen für Threads
        Console.WriteLine("\nParallele Verarbeitung mit Threads (isolierte Datenstrukturen):");
        long parallelTimeCommonData = RunParallelCommonData(iterations);
        
        // Parallele Verarbeitung mit isolierten Datenstrukturen für Threads
        Console.WriteLine("\nParallele Verarbeitung mit Threads (isolierte Datenstrukturen):");
        long parallelTimeSeparateData = RunParallelSeparateData(iterations);

        // Ausgabe der Vergleichsergebnisse
        Console.WriteLine($"\n\nSequentielle Gesamtzeit: {sequentialTime} ms");
        Console.WriteLine($"Parallele Gesamtzeit: {parallelTimeCommonData} ms");
        Console.WriteLine($"Parallele Gesamtzeit: {parallelTimeSeparateData} ms");
    }

    // Methode zur sequentiellen Ausführung
    private static long RunSequential(int iterations)
    {
        long totalElapsedTime = 0;

        for (int i = 1; i <= iterations; i++)
        {
            long elapsedTime = ProcessFileAndMeasureTime(i, Method.Sequential); // Sequentielle Verarbeitung
            totalElapsedTime += elapsedTime;
        }

        Console.WriteLine($"\nDurchschnittliche Zeit (sequentiell) für {iterations} Iterationen: {totalElapsedTime / (double)iterations} ms");
        return totalElapsedTime;
    }

    // Methode zur parallelen Ausführung mit Threads
    private static long RunParallelCommonData(int iterations)
    {
        long totalElapsedTime = 0;
        for (int i = 1; i <= iterations; i++)
        {
            long elapsedTime = ProcessFileAndMeasureTime(i, Method.ParallelCommonData); // Parallele Verarbeitung
            totalElapsedTime += elapsedTime;
        }

        Console.WriteLine($"\nDurchschnittliche Zeit (parallel, common data) für {iterations} Iterationen: {totalElapsedTime / (double)iterations} ms");
        return totalElapsedTime;
    }
    
    private static long RunParallelSeparateData(int iterations)
    {
        long totalElapsedTime = 0;
        for (int i = 1; i <= iterations; i++)
        {
            long elapsedTime = ProcessFileAndMeasureTime(i, Method.ParallelSeparatedDate); // Parallele Verarbeitung
            totalElapsedTime += elapsedTime;
        }

        Console.WriteLine($"\nDurchschnittliche Zeit (parallel, separate data) für {iterations} Iterationen: {totalElapsedTime / (double)iterations} ms");
        return totalElapsedTime;
    }

    // Methode zur Verarbeitung der Datei und Zeitmessung (sequentiell oder parallel)
    private static long ProcessFileAndMeasureTime(int iteration, Method method)
    {
        string methodInfo;
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.Start();

        switch (method)
        {
            case Method.Sequential:
                // Sequentielle Auswertung
                ProcessFileSequential(iteration); 
                methodInfo = "(sequentiell)";
                break;
            case Method.ParallelCommonData:
                // Parallele Auswertung mit Threads und geteilten Daten
                ProcessFileParallelWithThreadsAndCommonData(iteration); 
                methodInfo = "(parallel, common data)";
                break;
            case Method.ParallelSeparatedDate:
                // Parallele Auswertung mit Threads und separate Daten
                ProcessFileParallelWithThreadsAndSeparateData(iteration); 
                methodInfo = "(parallel, separate data)";
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(method), method, null);
        }

        stopwatch.Stop();
        Console.WriteLine($"Zeit für Iteration {iteration} {methodInfo}: {stopwatch.ElapsedMilliseconds} ms\n");
        return stopwatch.ElapsedMilliseconds;
    }

    // Methode zur parallelen Verarbeitung der Datei mit isolierten Datenstrukturen in Threads
    private static void ProcessFileParallelWithThreadsAndSeparateData(int iteration)
    {
        // Textdatei einlesen
        string text = ReadFile(FILE_PATH, FACTOR); // File.ReadAllText(filePath);

        // Wörter extrahieren, in Kleinbuchstaben umwandeln und nach Leerzeichen trennen
        string[] words = text
            .ToLower()
            .Split(new char[] { ' ', '\r', '\n', '\t', ',', '.', ';', ':', '-', '_', '!', '?' }, StringSplitOptions.RemoveEmptyEntries);

        // Anzahl der Threads und Aufteilung der Arbeit
        int threadCount = 8;
        int wordsPerThread = words.Length / threadCount;
        Thread[] threads = new Thread[threadCount];

        // Liste, um die Ergebnisse jedes Threads zu speichern
        List<Dictionary<string, int>> threadWordFrequencies = new List<Dictionary<string, int>>(threadCount);

        // Initialisieren der Wörterbücher für jeden Thread
        for (int t = 0; t < threadCount; t++)
        {
            threadWordFrequencies.Add(new Dictionary<string, int>());
        }

        for (int t = 0; t < threadCount; t++)
        {
            int start = t * wordsPerThread;
            int end = (t == threadCount - 1) ? words.Length : (t + 1) * wordsPerThread;

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
        string text = ReadFile(FILE_PATH, FACTOR); // File.ReadAllText(filePath);

        // Wörter extrahieren, in Kleinbuchstaben umwandeln und nach Leerzeichen trennen
        var words = text
            .ToLower()
            .Split(new char[] { ' ', '\r', '\n', '\t', ',', '.', ';', ':', '-', '_', '!', '?' }, StringSplitOptions.RemoveEmptyEntries);

        // Wörterbuch zur Zählung der Häufigkeit
        Dictionary<string, int> wordFrequency = new Dictionary<string, int>();
        object lockObject = new object(); // Lock-Objekt für den Zugriff auf das Wörterbuch

        // Anzahl der Threads und Aufteilung der Arbeit
        int threadCount = 4;
        int wordsPerThread = words.Length / threadCount;
        Thread[] threads = new Thread[threadCount];

        for (int t = 0; t < threadCount; t++)
        {
            int start = t * wordsPerThread;
            int end = (t == threadCount - 1) ? words.Length : (t + 1) * wordsPerThread;

            // Jeder Thread verarbeitet einen Teil der Wörter
            threads[t] = new Thread(() =>
            {
                Dictionary<string, int> localWordFrequency = new Dictionary<string, int>();

                for (int i = start; i < end; i++)
                {
                    string word = words[i];
                    if (!localWordFrequency.TryAdd(word, 1))
                    {
                        localWordFrequency[word]++;
                    }
                }

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
        string text = ReadFile(FILE_PATH, FACTOR); // File.ReadAllText(filePath);

        // Wörter extrahieren, in Kleinbuchstaben umwandeln und nach Leerzeichen trennen
        var words = text
            .ToLower()
            .Split(new char[] { ' ', '\r', '\n', '\t', ',', '.', ';', ':', '-', '_', '!', '?' }, StringSplitOptions.RemoveEmptyEntries);

        // Wörter zählen
        Dictionary<string, int> wordFrequency = new Dictionary<string, int>();

        foreach (var word in words)
        {
            if (!wordFrequency.TryAdd(word, 1))
            {
                wordFrequency[word]++;
            }
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

    private static string ReadFile(string filePath, int factor = 1)
    {
        string text = File.ReadAllText(filePath);

        for (int i = 1; i < factor; i++)
        {
            text += $" {text}";
        }

        return text;
    }
}

public enum Method
{
    Sequential,
    ParallelCommonData,
    ParallelSeparatedDate
}
