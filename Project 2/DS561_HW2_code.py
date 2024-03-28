import numpy as np
from google.cloud import storage
import re


# Directed Graph class
class DiGraph:
    
    #initialize empty graph
    def __init__(self):
        self.graph = {}

    def add_node(self, node):
        # Adds a node to the graph if it doesn't already exist
        if node not in self.graph:
            self.graph[node] = set()

    def add_edge(self, src_node, dst_node):
        #Adds a directed edge from src_node to dst_node
        self.add_node(src_node)
        self.add_node(dst_node)
        self.graph[src_node].add(dst_node)

    def get_outgoing_nodes(self, node):
        #Returns nodes that the given node points to
        return self.graph.get(node, set())

    def get_incoming_nodes(self, node):
        #Returns nodes that point to the given node
        return {k for k, v in self.graph.items() if node in v}

    def nodes(self):
        #Returns all nodes in the graph
        return self.graph.keys()

    def __str__(self):
        #String representation of the graph
        return "\n".join([f"{node} -> {', '.join(map(str, neighbors))}" for node, neighbors in self.graph.items()])




#nitializes a connection to specified GCS bucket and returns it
def initialize_storage_client(bucket_name):
    client = storage.Client.create_anonymous_client()
    return client.bucket(bucket_name)


# Iterates over each file in the specified GCS bucket, extracts outgoing links from each file's content, 
# constructs a directed graph based on these links, and returns the graph along with statistics on the links
def build_graph(bucket):
    outgoing_links = []
    incoming_links = {}
    G = DiGraph()
    
    #Filtering for valid files within a specified directory
    blobs = list(bucket.list_blobs(prefix="Serena_Directory/ds561_hw2_pythonfiles/"))
    valid_files = set(blob.name.split("/")[-1].replace(".html", "") for blob in blobs)
    
    #identify links within the HTML content
    link_pattern = re.compile(r'<a\s+href="([^"]+)"', re.IGNORECASE)

    for blob in blobs:
        content = blob.download_as_text()
        
        links = [match.group(1).replace(".html", "") for match in link_pattern.finditer(content)]
        links = [link for link in links if link in valid_files]
        
        file_name = blob.name.split("/")[-1].replace(".html", "")
        outgoing_links.append(len(links))
        
        G.add_node(file_name)
        for link in links:
            G.add_edge(file_name, link)
            
    for node in G.nodes():
        incoming_links[node] = len(G.get_incoming_nodes(node))
        
    return G, outgoing_links, incoming_links

#Prints statistics about the number of incoming and outgoing links for each node in the graph
def print_statistics(G):
    outgoing_links = [len(G.get_outgoing_nodes(node)) for node in G.nodes()]
    incoming_links = {node: len(G.get_incoming_nodes(node)) for node in G.nodes()}

    avg_outgoing = np.mean(outgoing_links)
    median_outgoing = np.median(outgoing_links)
    max_outgoing = np.max(outgoing_links)
    min_outgoing = np.min(outgoing_links)
    quintiles_outgoing = np.percentile(outgoing_links, [20, 40, 60, 80, 100])

    avg_incoming = np.mean(list(incoming_links.values()))
    median_incoming = np.median(list(incoming_links.values()))
    max_incoming = np.max(list(incoming_links.values()))
    min_incoming = np.min(list(incoming_links.values()))
    quintiles_incoming = np.percentile(list(incoming_links.values()), [20, 40, 60, 80, 100])

    print("Average outgoing:", avg_outgoing)
    print("Median outgoing:", median_outgoing)
    print("Max outgoing:", max_outgoing)
    print("Min outgoing:", min_outgoing)
    print("Quintiles outgoing:", quintiles_outgoing)
    print("Average incoming:", avg_incoming)
    print("Median incoming:", median_incoming)
    print("Max incoming:", max_incoming)
    print("Min incoming:", min_incoming)
    print("Quintiles incoming:", quintiles_incoming)

#Calculates the PageRank of each node in the graph using an iterative method
#Returns a dictionary with nodes as keys and their corresponding PageRank values as values
def original_iterative_pagerank(G, damping=0.85, max_iter=10000):
    N = len(G.nodes())
    pr = {node: 1.0/N for node in G.nodes()}

    for _ in range(max_iter):
        new_pr = {}
        for node in G.nodes():
            # For each node, consider the nodes linking to it (predecessors)
            preds = G.get_incoming_nodes(node)
            #distribute the PageRank of dangling nodes equally among all nodes in the graph
            dangling_pr = sum(pr[pred] for pred in G.nodes() if not G.get_outgoing_nodes(pred)) / N
            total_for_node = sum(pr[pred] / len(G.get_outgoing_nodes(pred)) if G.get_outgoing_nodes(pred) else dangling_pr for pred in preds)
            new_pr[node] = (1 - damping)/N + damping * total_for_node

        # Normalization step
        s = sum(new_pr.values())
        for node in G.nodes():
            new_pr[node] = new_pr[node] / s

        # Check for convergence 
        if sum(abs(new_pr[node]-pr[node]) for node in G.nodes()) < 0.005:
            break

        pr = new_pr

    return pr


def main():
    
    bucket = initialize_storage_client("serena_ds561_hw2_bucket")
    # Construct graph of the pages
    G, outgoing_links, incoming_links = build_graph(bucket)

    # Average, Median, Max, Min and Quintiles of incoming and outgoing links across all the files
    print_statistics(G)
    

    # Code the original iterative pagerank algorithm
    pagerank_iterative = original_iterative_pagerank(G)
    top_pages_iterative = sorted(pagerank_iterative.items(), key=lambda x: x[1], reverse=True)[:5]
    print("Top 5 pages by iterative PageRank:", top_pages_iterative)

if __name__ == "__main__":
    main()
