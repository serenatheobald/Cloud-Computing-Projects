import networkx as nx
import numpy as np
from google.cloud import storage
import os
import re

#nitializes a connection to specified GCS bucket and returns it
def initialize_storage_client(bucket_name):
    client = storage.Client()
    return client.bucket(bucket_name)


#Iterates over each file in the specified GCS bucket.
#Extracts outgoing links from each file's content.
#Constructs a directed graph (nx.DiGraph()) based on these links.
#Returns the graph, a list of counts of outgoing links for each file, and a dictionary of incoming link counts.
def build_graph(bucket):
    outgoing_links = []
    incoming_links = {}
    G = nx.DiGraph()
    
    # List valid files
    blobs = list(bucket.list_blobs(prefix="Serena_Directory/ds561_hw2_pythonfiles/"))
    valid_files = set(blob.name.split("/")[-1].replace(".html", "") for blob in blobs)

    # Link extraction pattern
    link_pattern = re.compile(r'<a\s+href="([^"]+)"', re.IGNORECASE)

    # Iterate over each file in the GCS bucket
    for blob in blobs:
        content = blob.download_as_text()
        
        # Extract outgoing links
        links = [match.group(1).replace(".html", "") for match in link_pattern.finditer(content)]
        
        # Filter out invalid links
        links = [link for link in links if link in valid_files]
        
        file_name = blob.name.split("/")[-1].replace(".html", "")
        outgoing_links.append(len(links))
        
        G.add_node(file_name)
        for link in links:
            G.add_edge(file_name, link)
            
    for node in G.nodes():
        incoming_links[node] = len(G.pred[node])
        
    return G, outgoing_links, incoming_links

#Calculates and prints various statistics
#outgoing and incoming links, including averages, medians, maxima, minima, and quintiles
def print_statistics(outgoing_links, incoming_links):
    # Outgoing link statistics
    avg_outgoing = np.mean(outgoing_links)
    median_outgoing = np.median(outgoing_links)
    max_outgoing = np.max(outgoing_links)
    min_outgoing = np.min(outgoing_links)
    quintiles_outgoing = np.percentile(outgoing_links, [20, 40, 60, 80, 100])

    # Incoming link statistics
    avg_incoming = np.mean(list(incoming_links.values()))
    median_incoming = np.median(list(incoming_links.values()))
    max_incoming = np.max(list(incoming_links.values()))
    min_incoming = np.min(list(incoming_links.values()))
    quintiles_incoming = np.percentile(list(incoming_links.values()), [20, 40, 60, 80, 100])

    # Print the calculated statistics
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
def original_iterative_pagerank(G, damping=0.85, max_iter=1000):
    N = len(G)
    pr = {node: 1.0/N for node in G.nodes()}

    for _ in range(max_iter):
        new_pr = {}
        for node in G.nodes():
            total_for_node = sum([pr[pred] / len(G[pred]) if G[pred] else 0 for pred in G.pred[node]])
            new_pr[node] = (1 - damping)/N + damping * total_for_node

        # Normalization step
        s = sum(new_pr.values())
        for node in G.nodes():
            new_pr[node] = new_pr[node] / s

        # Check for convergence using L1 norm
        if sum(abs(new_pr[node]-pr[node]) for node in G.nodes()) < 0.005:
            break
        
        pr = new_pr

    return pr

def main():
    
    # Set the environment variable for authentication
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/Users/serenatheobald/Downloads/ds-561-first-project-a6047833252d.json"

    bucket = initialize_storage_client("serena_ds561_hw2_bucket")
    #construct graph of the pages
    G, outgoing_links, incoming_links = build_graph(bucket)

    #Average, Median, Max, Min and Quintiles of incoming and outgoing links across all the files
    print_statistics(outgoing_links, incoming_links)
    
    #compute the pagerank after constructing graph of pages
    #output the top 5 pages by their pagerank score
    pagerank = nx.pagerank(G, alpha=0.85, tol=0.005)
    top_pages = sorted(pagerank.items(), key=lambda x: x[1], reverse=True)[:5]
    print("Top 5 pages by PageRank:", top_pages)

    #Code the original iterative pagerank algorithm
    pagerank_iterative = original_iterative_pagerank(G)
    top_pages_iterative = sorted(pagerank_iterative.items(), key=lambda x: x[1], reverse=True)[:5]
    print("Top 5 pages by iterative PageRank:", top_pages_iterative)

if __name__ == "__main__":
    main()
