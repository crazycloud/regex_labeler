import logging
import json 
import re
import collections
import io
import os 
import csv
import tempfile
import glob 
import pathlib
import pickle 
import shutil

# Create a custom logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

FLAGS = collections.namedtuple('FLAGS', ['split', 'dictionary', 'output_dir','one_document_per_line','export_format','skip_blank_annotations'])

# A label pattern representing a dictionary's csv line.
LabelPattern = collections.namedtuple('LabelPattern',
                                      ['pattern', 'label', 'matching_mode','matching_group'])

# Valid matching mode in LabelPattern
EXACT_MATCH = 'e'
IGNORE_CASE = 'i'
REGEX = 'r'
MATCHING_MODES = [EXACT_MATCH, IGNORE_CASE, REGEX]


MAX_EXAMPLE_SIZE_IN_BYTES = None
MAX_ANNOTATION_TOKENS = None
MAX_LABEL_LENGTH = None

# Represents an annotation in the example. The annotation is in range
# [start, end) with the label. The offsets are in unicode code points.
Annotation = collections.namedtuple('Annotation', ['start', 'end', 'label'])

def _ConvertOneExample(example_content):
    """Convert a pure text example into a jsonl string."""
    json_obj = {
          'annotations': [],
          'text_snippet': {
              'content': example_content
          },
      }
    return json.dumps(json_obj, ensure_ascii=False) + '\n'


def ConvertOneFile(import_file, auto_split, full_output_jsonl):
    """Convert one file and write into output_jsonl.

    one_document_per_line - create one jsonl example from each line in the document
    one_file_per_document - if document length < maximum length - create one example from the document by concatenating 
    all line in the document 
    if document length > maximum length, join lines up to max_length and then create create a new example 

    Args:
      import_file: A ImportFile to convert
      auto_split: True/False, whether to auto split file if it is too large
      full_output_jsonl: The full file path of the output jsonl
    """
    json_lines = []  # all converted json lines
    example_content = ''  # the current content in an example
    blank_lines = 0
    long_lines = 0
    for line in io.open(import_file, 'r', encoding='utf-8'):
        line = line.strip()
        if not line:  # skip blank lines
            blank_lines += 1
            continue
        
        if MAX_EXAMPLE_SIZE_IN_BYTES and len(line) > MAX_EXAMPLE_SIZE_IN_BYTES:  # too long a line
            long_lines += 1
            continue
        
        if FLAGS.one_document_per_line:
            json_lines.append(_ConvertOneExample(line))
            continue
  
        if (auto_split and MAX_EXAMPLE_SIZE_IN_BYTES and example_content and
            # use >= to leave a place for '\n'
            len(example_content) + len(line) >= MAX_EXAMPLE_SIZE_IN_BYTES):
            json_lines.append(_ConvertOneExample(example_content))
            example_content = ''
        example_content = '\n'.join(filter(None, [example_content, line]))

    if example_content:
        json_lines.append(_ConvertOneExample(example_content))

    with io.open(full_output_jsonl, 'w', encoding='utf-8') as output_file:
        output_file.writelines(json_lines)

    extra_info = ''
    if blank_lines or long_lines:
        extra_info = ' (with%s%s skipped)' % (
          ' %d blank lines' % blank_lines if blank_lines else '',
          ' %d long lines' % long_lines if long_lines else '')
    logger.info('Converted %s to %s%s' % (import_file, full_output_jsonl, extra_info))
          
def _HasOverlap(a1, a2):
    """Check if the 2 annotations overlap."""
    return (a1.start >= a2.start and a1.start < a2.end or
          a1.end > a2.start and a1.end <= a2.end)


def _AnnotationToJson(annotation):
    return {
        'text_extraction': {
            'text_segment': {
                'start_offset': annotation.start,
                'end_offset': annotation.end
            }
        },
        'display_name': annotation.label
    }
    

def _AnnotateExample(example, label_patterns):
    """Annotate an example by a list of label_patterns.

        Args:
          example: one example as json object (one line in the jsonl file)
          label_patterns: a list of LabelPattern parsed from dictionary

        Returns:
          The annotated example as json object
    """
    example_text = example['text_snippet']['content']
    annotations = [
        Annotation(start=a['text_extraction']['text_segment']['start_offset'],
                       end=a['text_extraction']['text_segment']['end_offset'],
                       label=a['display_name'])
        for a in example['annotations']
    ]

    def _AddAnnotation(annotation):
        for a in annotations:
            if _HasOverlap(annotation, a):
                return False
        annotations.append(annotation)
        return True

    for label_pattern in label_patterns:
        logger.debug('Matching pattern "%s"(%s) to label "%s with matching group (%s)"',
                 label_pattern.pattern, label_pattern.matching_mode,
                 label_pattern.label, label_pattern.matching_group)
        if label_pattern.matching_mode == EXACT_MATCH:
          # Exact match word on the boundary
            regex = u'\\b%s\\b' % label_pattern.pattern
            matcher = re.finditer(regex, example_text, re.UNICODE)
        elif label_pattern.matching_mode == IGNORE_CASE:
            # Ignore case match word on the boundary
            regex = u'%s' % label_pattern.pattern
            #logger.info(regex)
            matcher = re.finditer(regex, example_text, re.UNICODE | re.IGNORECASE)
        elif label_pattern.matching_mode == REGEX:
            # Use regex to match word (not necessarily on word boundary)
            regex = u'%s' % label_pattern.pattern
            
            matcher = re.finditer(regex, example_text, re.UNICODE)
        
        if len(label_pattern.matching_group):
            label_group = int(label_pattern.matching_group)
        else:
            label_group = 0
        for match in matcher:
            if match.start(label_group) >= match.end(label_group):
                logging.warning('  Skipped empty match at %d', match.start())
                continue
            if len(match.groups()) < int(label_group) or not match.group(int(label_group)):
                logging.warning('  Skipped empty match group at %d', match.start())
                continue
            annotation = Annotation(match.start(label_group), match.end(label_group), label_pattern.label)
            is_added = _AddAnnotation(annotation)
            logger.info('  Matched {} at {} {}'.format(match.group(1), match.start(label_group),
                             'but skipped' if not is_added else ''))
            if is_added:
                # Try validating annotation, but not blocking
                tokens = len(list(filter(None, re.split(r'\s+', match.group(1)))))
                if not tokens or (MAX_ANNOTATION_TOKENS and tokens > MAX_ANNOTATION_TOKENS):
                    logging.warning(
                        '  Annotation "%s" is probably invalid and get '
                        'skipped later. The number of tokens should be '
                        'in range [1, 10].', match.group(1))

    example['annotations'] = [_AnnotationToJson(a) for a in annotations]
    return example

def _ParseDictionary(file_path):
    """Parse the dictionary from a file.

      Args:
        file_path: The path of local file.

      Returns:
        A list of LabelPattern in the same order as listed in the file.
    """
    local_file_path = file_path
    # Download file to local if it is a GCS file
    label_patterns = []
    pattern_and_mode = {}
    with io.open(local_file_path, 'r', encoding='utf-8') as dict_file:
        csv_reader = csv.reader(dict_file)
        for row in csv_reader:
            if len(row) < 2:
                logging.warning('Skipped malformed line%d "%s" in dictionary',
                              csv_reader.line_num, row)
                continue
            pattern = row[0].strip()
            label = row[1].strip()
            mode = row[2].strip().lower()
            group = row[3].strip() if len(row) > 3 else ''
            mode = mode if mode in MATCHING_MODES else EXACT_MATCH
            if not pattern or not label:
                logging.warning('Skipped malformed line%d "%s" in dictionary',
                              csv_reader.line_num, row)
                continue
            if (pattern, mode) in pattern_and_mode:
                logging.warning(
                      'Skipped duplicate pattern in line%d "%s" in dictionary',
                      csv_reader.line_num, row)
                continue
            # Validate label
            m = re.match(r'\w+', label)
            if m and MAX_LABEL_LENGTH and len(m.group(0)) > MAX_LABEL_LENGTH:
                logging.warning(
                      'Skipped invalid label in line%d "%s" in dictionary. '
                      'Valid labels are at most %d characters long, '
                      'with characters in [a-zA-Z0-9_].', csv_reader.line_num, row,
                      MAX_LABEL_LENGTH)
                continue
            pattern_and_mode[(pattern, mode)] = 1
            label_patterns.append(LabelPattern(pattern, label, mode, group))

    logger.info('Parsed %d label patterns from %s', len(label_patterns),
                 file_path)
    return label_patterns

def AnnotateFiles(import_files):
    """Annotate import files based on a dictionary if it is there.

      Args:
        import_files: A list of ImportFile to be annotated

      Returns:
        A list of ImportFile with the jsonl annotated.
      """
    if not FLAGS.dictionary:
        return import_files
    label_patterns = _ParseDictionary(FLAGS.dictionary)
    if not label_patterns:
        return import_files

    logger.info('Annotating jsonl files with dictionary in {}'.format(FLAGS.dictionary))
    for import_file in import_files:
        jsonl_local_path = import_file
        if not jsonl_local_path or not os.path.isfile(jsonl_local_path):
              continue

        json_lines = []  # all annotated json lines
        with io.open(jsonl_local_path, 'r', encoding='utf-8') as jsonl_file:
              for line in jsonl_file:
                    example = json.loads(line)
                    _AnnotateExample(example, label_patterns)
                    json_lines.append(json.dumps(example, ensure_ascii=False) + '\n')
        with io.open(jsonl_local_path, 'w', encoding='utf-8') as output_file:
            output_file.writelines(json_lines)
            logger.info('Annotated %s', jsonl_local_path)

    return import_files



def ConvertFiles(import_files, output_dir):
    """Take a list of ImportFile and convert them into jsonl locally.

      Args:
        import_files: A list of ImportFile to be converted.

      Returns:
        A list of ImportFile converted to jsonl locally
        (with local_output_jsonl pointing to a temp file).
    """
    jsonl_files=[]
    temp_dir = output_dir
    # A map of filename -> count to avoid filename conflicts.
    filename_root_dict = {}
    for import_file in import_files:
        basename = os.path.basename(import_file)
        filename_root, filename_ext = os.path.splitext(basename)
        output_jsonl = filename_root + '.jsonl'
        # Generates a unique output file name if it already exists
        num_occur = 1
        if filename_root in filename_root_dict:
            num_occur = filename_root_dict[filename_root] + 1
            output_jsonl = filename_root + str(num_occur) + '.jsonl'
        filename_root_dict[filename_root] = num_occur

        full_output_jsonl = os.path.join(temp_dir, output_jsonl)
        if filename_ext == '.jsonl':
          # For jsonl, we assume it is already converted and simply copy it.
          shutil.copyfile(import_file, full_output_jsonl)
        else:
            filesize = os.path.getsize(import_file)
            if MAX_EXAMPLE_SIZE_IN_BYTES and filesize > MAX_EXAMPLE_SIZE_IN_BYTES and not FLAGS.split:
                logger.info(
                    '{} is skipped as it exceeds the max size limit ({} bytes). '
                    'Please truncate or split it. Or rerun with "-s" to auto split it.'
                    .format(import_file.original_filepath, MAX_EXAMPLE_SIZE_IN_BYTES))
                continue
            ConvertOneFile(import_file, FLAGS.split, full_output_jsonl)

        jsonl_files.append(full_output_jsonl)

    return jsonl_files

def export(jsonl_files, export_format, output_csv):
    if output_csv:
        fcsv = open(output_csv,'w')
    spacy_error =False
    try:
        import spacy 
        from spacy.gold import biluo_tags_from_offsets
        nlp = spacy.load("en_core_web_sm")
    except ImportError:
        spacy_error = True
        raise
        
    new_examples = []
    for file in jsonl_files:
        #print(file)
        for example in io.open(file):
            jsonl_example = json.loads(example)
            if export_format == 'SPACY' or export_format =='BILUO':
                new_annotations = []
                text = jsonl_example['text_snippet']['content']
                for annotation in jsonl_example['annotations']:
                    start_offset = annotation['text_extraction']['text_segment']['start_offset']
                    end_offset = annotation['text_extraction']['text_segment']['end_offset']
                    label = annotation['display_name']
                    new_annotations.append((start_offset, end_offset, label))
                if output_csv:
                    ann_value = '\n'.join([text[v[0]:v[1]]+' - '+v[2] for v in new_annotations])
                    fcsv.write('"'+text.replace('"','')+'","'+ann_value+'"\n')
                if FLAGS.skip_blank_annotations and len(new_annotations) <= 0:
                    continue
                if  export_format == 'BILUO':
                    if not spacy_error:
                        doc = nlp(text)
                        tags = biluo_tags_from_offsets(doc, new_annotations)
                        tokens = [token.text for token in doc]
                        new_examples.append((tokens, tags)) 
                elif export_format == 'SPACY':
                    new_examples.append((text,{'entities':new_annotations}))
    fcsv.close()
    return new_examples

def generate(import_files, regex_file, export_format, export_dir, skip_blank_annotations=True, split=False, maximum_example_length= None):
    """
    import_files - list of input files with one example per line
    regex_file - list of entities with corresponding regex
    export_format - SPACY, BILUO
    export_dir - 
    skip_blank_annotations - skip examples if there are no entities in the example
    split - split the example if the length is more than 
    maximum_example_length - maximum length of the example to split the examples
    """
    FLAGS.split = split
    FLAGS.one_document_per_line = True
    FLAGS.output_dir = export_dir
    FLAGS.skip_blank_annotations = skip_blank_annotations
    if split:
        if not maximum_example_length:
            logger.error('maximum example length required to split the examples')
            raise Exception('Maximum Example Length required to split the examples')
        MAX_EXAMPLE_SIZE_IN_BYTES = maximum_example_length
        
    

    ##### 
    FLAGS.export_format = export_format
    FLAGS.dictionary = regex_file
    pathlib.Path(export_dir).mkdir(exist_ok=True)
    converted_files = ConvertFiles(import_files, FLAGS.output_dir)
    annotated_files = AnnotateFiles(converted_files)
    result = export(annotated_files, FLAGS.export_format, output_csv="label-value.csv")
    if export_format =='SPACY':
        with open(os.path.join(FLAGS.output_dir,'training_data_spacy.pickle'), 'wb') as handle:
            pickle.dump(result, handle, protocol=pickle.HIGHEST_PROTOCOL)
    return result