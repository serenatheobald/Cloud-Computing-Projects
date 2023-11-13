import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.fileio import MatchFiles, ReadMatches
from bs4 import BeautifulSoup
from apache_beam.transforms.combiners import Top
from apache_beam.metrics import Metrics, MetricsFilter



class ExtractLinksFn(beam.DoFn):
    def __init__(self):
        self.outgoing_links_counter = Metrics.counter(self.__class__, 'outgoing_links')
        self.incoming_links_counter = Metrics.counter(self.__class__, 'incoming_links')



    def process(self, element):
        file_name, file_contents = element
        soup = BeautifulSoup(file_contents, 'html.parser')
        
        # Unique set for each document to avoid double-counting the same link in one document
        unique_links = set()

        for a_tag in soup.find_all('a', href=True):
            link = a_tag['href'].strip()
            if not link.startswith('http'):
                normalized_link = link.lstrip('/')

                # Only count unique links for outgoing links per document
                if normalized_link not in unique_links:
                    self.outgoing_links_counter.inc()
                    yield file_name, (normalized_link, 1)
                    unique_links.add(normalized_link)
                
                # For incoming links, count every link because each represents
                # a different source document pointing to the target
                self.incoming_links_counter.inc()
                yield normalized_link, (file_name, 1)
        

def invert_links(element):
    file_name, link = element
    return link, file_name  # Invert to (link, file_name)

def debug_log(element):
    print(f"Debug Log - Element: {element}, Type: {type(element)}")
    return element

def print_to_console(element):
    print(element)
    return element

def format_and_print_top_links(top_links, link_type):
    print(f"Top 5 files with the most {link_type} links:")
    for filename, count in top_links:
        print(f"{filename}: {count}")

def main():
    options = PipelineOptions(
        runner='DirectRunner',  #change to DataFlowRunner to run on GCP
        project= 'ds-561-first-project',
        job_name='html-link-counter'
    )

    input_files_path = 'gs://serena_ds561_hw2_bucket/Serena_Directory/ds561_hw2_pythonfiles/*'
    output_path = 'gs://serena_ds561_hw2_bucket/Serena_Directory/output/'

    #input_files_path = 'gs://serena_ds561_hw2_bucket/Test_Directory/*'
    #output_path = 'gs://serena_ds561_hw2_bucket/Test_Directory/output/'
    
    with beam.Pipeline(options=options) as p:
        files = (p
                 | 'MatchFiles' >> MatchFiles(input_files_path)
                 | 'ReadMatches' >> ReadMatches()
                 | 'MapToFileNameAndContent' >> beam.Map(lambda file: (file.metadata.path.split('/')[-1], file.read_utf8()))
                 )

        links = (files | 'ExtractLinks' >> beam.ParDo(ExtractLinksFn()))

        # Counting Outgoing Links
        outgoing_links = (links
                  | 'ExtractOutgoingLinks' >> beam.Map(lambda x: (x[0], x[1][1]))
                  | 'CountOutgoingLinks' >> beam.CombinePerKey(sum))

        # Counting Incoming Links
        incoming_links = (links
                  | 'ExtractIncomingLinks' >> beam.Map(lambda x: (x[1][0], x[1][1]))
                  | 'CountIncomingLinks' >> beam.CombinePerKey(sum))

        # Calculate the top 5 files with the most outgoing links
        top_outgoing = (outgoing_links
                        | 'GetTop5Outgoing' >> beam.CombineGlobally(
                            beam.combiners.TopCombineFn(5, key=lambda x: x[1]))
                        .without_defaults())

        # Calculate the top 5 files with the most incoming links
        top_incoming = (incoming_links
                        | 'GetTop5Incoming' >> beam.CombineGlobally(
                            beam.combiners.TopCombineFn(5, key=lambda x: x[1]))
                        .without_defaults())

        # Apply a Map transform to format and print the results
        top_outgoing | 'FormatAndPrintTopOutgoing' >> beam.Map(format_and_print_top_links, link_type='outgoing')
        top_incoming | 'FormatAndPrintTopIncoming' >> beam.Map(format_and_print_top_links, link_type='incoming')


if __name__ == '__main__':
    main()
    
    
    
