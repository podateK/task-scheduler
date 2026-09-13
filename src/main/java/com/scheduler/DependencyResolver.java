package com.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class DependencyResolver {

    private static final Logger log = LoggerFactory.getLogger(DependencyResolver.class);

    public List<String> resolve(Map<String, Task> tasks) {
        Map<String, Set<String>> graph = buildGraph(tasks);
        detectCycles(graph);
        return topologicalSort(graph);
    }

    public List<String> resolveSubgraph(Map<String, Task> tasks, Set<String> rootIds) {
        Set<String> reachable = collectReachable(tasks, rootIds);
        Map<String, Set<String>> subgraph = buildFilteredGraph(tasks, reachable);
        return topologicalSort(subgraph);
    }

    public boolean wouldCreateCycle(Map<String, Task> tasks, String taskId, String dependencyId) {
        Map<String, Set<String>> graph = buildGraph(tasks);
        graph.computeIfAbsent(taskId, k -> new HashSet<>()).add(dependencyId);
        return hasCycle(graph);
    }

    public Set<List<String>> findAllCycles(Map<String, Task> tasks) {
        Map<String, Set<String>> graph = buildGraph(tasks);
        return findCycles(graph);
    }

    private Map<String, Set<String>> buildGraph(Map<String, Task> tasks) {
        Map<String, Set<String>> graph = new HashMap<>();
        for (Task task : tasks.values()) {
            graph.putIfAbsent(task.getId(), new HashSet<>());
            for (String dep : task.getDependencies()) {
                graph.computeIfAbsent(dep, k -> new HashSet<>());
                graph.get(dep).add(task.getId());
            }
        }
        return graph;
    }

    private Map<String, Set<String>> buildFilteredGraph(Map<String, Task> tasks, Set<String> nodeIds) {
        Map<String, Set<String>> graph = new HashMap<>();
        for (String id : nodeIds) {
            Task task = tasks.get(id);
            if (task != null) {
                Set<String> deps = task.getDependencies().stream()
                        .filter(nodeIds::contains)
                        .collect(Collectors.toSet());
                graph.put(id, deps);
            }
        }
        return graph;
    }

    private Set<String> collectReachable(Map<String, Task> tasks, Set<String> roots) {
        Set<String> visited = new LinkedHashSet<>();
        Deque<String> stack = new ArrayDeque<>(roots);
        while (!stack.isEmpty()) {
            String id = stack.poll();
            if (visited.add(id)) {
                Task task = tasks.get(id);
                if (task != null) {
                    task.getDependencies().stream()
                            .filter(dep -> !visited.contains(dep))
                            .forEach(stack::push);
                }
            }
        }
        return visited;
    }

    public void detectCycles(Map<String, Set<String>> graph) {
        Set<List<String>> cycles = findCycles(graph);
        if (!cycles.isEmpty()) {
            String cycleDescriptions = cycles.stream()
                    .map(cycle -> String.join(" -> ", cycle))
                    .collect(Collectors.joining("\n"));
            throw new CyclicDependencyException("Cycle detected:\n" + cycleDescriptions);
        }
    }

    private boolean hasCycle(Map<String, Set<String>> graph) {
        return !findCycles(graph).isEmpty();
    }

    private Set<List<String>> findCycles(Map<String, Set<String>> graph) {
        Set<List<String>> cycles = new HashSet<>();
        Set<String> visited = new HashSet<>();
        Set<String> inStack = new HashSet<>();
        Map<String, String> parents = new HashMap<>();

        for (String node : graph.keySet()) {
            if (!visited.contains(node)) {
                dfsCycleDetection(graph, node, visited, inStack, parents, cycles);
            }
        }
        return cycles;
    }

    private void dfsCycleDetection(Map<String, Set<String>> graph, String node,
                                    Set<String> visited, Set<String> inStack,
                                    Map<String, String> parents, Set<List<String>> cycles) {
        visited.add(node);
        inStack.add(node);

        for (String neighbor : graph.getOrDefault(node, Set.of())) {
            if (!visited.contains(neighbor)) {
                parents.put(neighbor, node);
                dfsCycleDetection(graph, neighbor, visited, inStack, parents, cycles);
            } else if (inStack.contains(neighbor)) {
                List<String> cycle = buildCyclePath(neighbor, node, parents);
                cycle.add(neighbor);
                cycles.add(cycle);
            }
        }
        inStack.remove(node);
    }

    private List<String> buildCyclePath(String start, String end, Map<String, String> parents) {
        List<String> path = new ArrayList<>();
        String current = end;
        while (!current.equals(start)) {
            path.addFirst(current);
            current = parents.get(current);
            if (current == null) break;
        }
        path.addFirst(start);
        return path;
    }

    private List<String> topologicalSort(Map<String, Set<String>> graph) {
        Map<String, Integer> inDegree = new HashMap<>();
        graph.forEach((node, deps) -> {
            inDegree.putIfAbsent(node, 0);
            for (String dep : deps) {
                inDegree.merge(dep, 1, Integer::sum);
            }
        });

        PriorityQueue<String> queue = new PriorityQueue<>(
                Comparator.comparingInt(inDegree::getOrDefault).thenComparing(Comparator.naturalOrder())
        );
        inDegree.forEach((node, degree) -> {
            if (degree == 0) queue.add(node);
        });

        List<String> sorted = new ArrayList<>();
        while (!queue.isEmpty()) {
            String node = queue.poll();
            sorted.add(node);
            for (String dependent : graph.keySet()) {
                if (graph.getOrDefault(dependent, Set.of()).contains(node)) {
                    int newDegree = inDegree.merge(dependent, -1, Integer::sum);
                    if (newDegree == 0) queue.add(dependent);
                }
            }
        }

        if (sorted.size() != graph.size()) {
            throw new CyclicDependencyException("Unable to perform topological sort - cycle detected");
        }
        return sorted;
    }

    public static class CyclicDependencyException extends RuntimeException {
        public CyclicDependencyException(String message) {
            super(message);
        }
    }
}
