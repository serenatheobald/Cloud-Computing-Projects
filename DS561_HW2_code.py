import networkx as nx
import numpy as np
from google.cloud import storage
import os


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

        
    # Iterate over each file in the GCS bucket
    blobs = list(bucket.list_blobs(prefix="Serena_Directory/ds561_hw2_pythonfiles/"))
    for blob in blobs:
        content = blob.download_as_text()
        
        # Extract outgoing links from each file's content
        links = [link.split('"')[1].replace(".html", "") for link in content.split('<a HREF="')[1:]]
        
        file_name = blob.name.split("/")[-1].replace(".html", "")
        #store the number of outgoing links for each file
        outgoing_links.append(len(links))
        
        # Adding nodes and edges to the graph G
        G.add_node(file_name)
        for link in links:
            #add an edge from the current file to the linked file to update graph
            G.add_edge(file_name, link)
            
            # Update incoming links
            if link not in G:
                G.add_node(link)
            incoming_links[link] = len(G.pred[link])
            
    return G, outgoing_links, incoming_links

#Calculates and prints various statistics
#outgoing and incoming links, including averages, medians, maxima, minima, and quintiles
def print_statistics(outgoing_links, incoming_links):
    avg_outgoing = np.mean(outgoing_links)
    print("Average outgoing:", avg_outgoing)
    
    median_outgoing = np.median(outgoing_links)
    print("Median outgoing:", median_outgoing)
    
    max_outgoing = np.max(outgoing_links)
    print("Max outgoing:", max_outgoing)
    
    min_outgoing = np.min(outgoing_links)
    print("Min outgoing:", min_outgoing)
    
    quintiles_outgoing = np.percentile(outgoing_links, [20, 40, 60, 80])
    print("Quintiles outgoing:", quintiles_outgoing)
    
    avg_incoming = np.mean(list(incoming_links.values()))
    print("Average incoming:", avg_incoming)
    
    median_incoming = np.median(list(incoming_links.values()))
    print("Median incoming:", median_incoming)
    
    max_incoming = np.max(list(incoming_links.values()))
    print("Max incoming:", max_incoming)
    
    min_incoming = np.min(list(incoming_links.values()))
    print("Min incoming:", min_incoming)
    
    quintiles_incoming = np.percentile(list(incoming_links.values()), [20, 40, 60, 80])
    print("Quintiles incoming:", quintiles_incoming)

#Calculates the PageRank of each node in the graph using an iterative method
#Returns a dictionary with nodes as keys and their corresponding PageRank values as values
def original_iterative_pagerank(G, damping=0.85, max_iter=1000):
    N = len(G)  # Number of nodes in the graph
    pr = {node: 1.0/N for node in G.nodes()}  # Initialize each node's PageRank to 1/N

    total_pr_prev = sum(pr.values())  # Initial total PageRank value

    for _ in range(max_iter):
        new_pr = {}
        for node in G.nodes():
            total_for_node = sum([pr[pred] / len(G[pred]) if G[pred] else 0 for pred in G.pred[node]])
            
            # PageRank formula here:
            new_pr[node] = (1 - damping)/N + damping * total_for_node
        
        total_pr_new = sum(new_pr.values())
        
        # Check for convergence
        if abs(total_pr_new - total_pr_prev) / total_pr_prev < 0.005:  # Change is less than 0.5%
            break
        
        pr = new_pr  # Update PageRank values for the next iteration
        total_pr_prev = total_pr_new  # Update the previous total PageRank value

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
