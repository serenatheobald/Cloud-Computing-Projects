import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.fileio import MatchFiles, ReadMatches
from apache_beam.transforms.combiners import Top
from apache_beam.metrics import Metrics, MetricsFilter
import logging
from datetime import datetime
from apache_beam.io import WriteToText



class ExtractLinksFn(beam.DoFn):
    def __init__(self):
        from bs4 import BeautifulSoup 
        self.BeautifulSoup = BeautifulSoup
        self.outgoing_links_counter = Metrics.counter(self.__class__, 'outgoing_links')
        self.incoming_links_counter = Metrics.counter(self.__class__, 'incoming_links')



    def process(self, element):
        file_name, file_contents = element
        #soup = BeautifulSoup(file_contents, 'html.parser')
        soup = self.BeautifulSoup(file_contents, 'html.parser')
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
    logging.info(f"Debug Log - Element: {element}, Type: {type(element)}")
    return element

def print_to_console(element):
    print(element)
    return element

def format_and_print_top_links(top_links, link_type):
    """Formats the top links for output."""
    formatted_lines = [f"Top 5 files with the most {link_type} links:"]
    for filename, count in top_links:
        formatted_lines.append(f"{filename}: {count}")
    return '\n'.join(formatted_lines)

def main():
    current_time = datetime.now().strftime("%Y%m%d%H%M%S")
    unique_job_name = f"html-link-counter-{current_time}"  # Unique job name
    options = PipelineOptions(
        runner='DataflowRunner',
        project= 'ds-561-first-project',
        job_name=unique_job_name,
        staging_location='gs://serena_ds561_hw2_bucket/Serena_Directory/staging',
        temp_location='gs://serena_ds561_hw2_bucket/Serena_Directory/temp',
        region='us-central1'

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
        # Writing the top outgoing links to GCS
        (top_outgoing 
         | 'FormatTopOutgoingLinks' >> beam.Map(format_and_print_top_links, link_type='outgoing')
         | 'WriteTopOutgoingToGCS' >> WriteToText(output_path + 'top_outgoing_links', file_name_suffix='.txt'))
    
        # Writing the top incoming links to GCS
        (top_incoming 
         | 'FormatTopIncomingLinks' >> beam.Map(format_and_print_top_links, link_type='incoming')
         | 'WriteTopIncomingToGCS' >> WriteToText(output_path + 'top_incoming_links', file_name_suffix='.txt'))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
    
    
    
