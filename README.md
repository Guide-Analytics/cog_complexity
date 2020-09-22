# Cognitive Complexity 

## Abstract:
- TBW (TO BE WRITTEN)

## PDF: 
- For more info: [CEO Cognitive Complexity](ceo_cog_complexity.pdf)

## Prerequisite:
- Recommended file name format: _author_names_.txt (with given reviews in each .txt file)
- Any unique identifier or file labels will be fine as long as each file is in .txt format
- Install requirements.txt via pip - essentially, it's just pyspark in there right now:
    - `pip install -r requirements.txt`

## Input:
- Default input: corpus words located in **dictionary** folder containing
    - strong.txt (strong words)
    - weak.txt (weak words)
    - comp.txt (comparative words - establishing comparisons)
    - diff.txt (difference words - capturing differentiation in thinking, drawing distinctions, or describing contrasts)
- Takes a directory of .txt files. The directory must be stored in **author_data**
- .txt must not contain any dataframes. Must be all texts

## Output:
- A dataframe with each .txt file name as an id/file name column 
- Results is saved in one .csv file. You may use other conversion file types (i.e. csv --> parquet, csv --> avro). Make that change either on a seperate program or in _line 158_
- Results are stored in **author_data/results**

## START: How to execute program 
- _While this program behaves more like a script, line 103-104 still act as the input command line prompt_
- `python3 cog_complexity_score.py [file_directory_input] [output_name]`
    - file_directory_input: _default: **author\_data** folder_. You may put your inputs inside here. If you wish to create your own input folder, simply create another folder inside **author_data** and specify it when running: `python3 cog_complexity_score.py folder_name`
    - output_name: output file name. Program will give you an arbitrary name if not given...


## OVERVIEW: How is it done - (per notes from CEO's Q&A calls):
- Our measure of differentiation language was the number of such words used divided by all of the [CEO's] Q&A words in a given call.
- Our measure of nuance leverages both valences: number of weak words divided by the total number of strong and weak words spoken by the [CEO's] in the Q&A part of the call. We added 0.01 to the numerator and 0.02 to the denominator to allow for meaningful values when either term was zero.
- Our final measure was the ratio of comparative words to all words spoken by the [CEO's] in the Q&A portion of a given conference call.
- Using this sample, we measured our three component indicators of [CEO's] cognitive complexity (differentiation, nuance, and comparison) for each call.
- Cognitive scores are located at the last 3 columns of the [pew_dataset], with the token count (word count) being placed in the front of the scores. Everything after column txt-wordcount, are newly added ([scientists_full_file_name]	file_name_id	[name]	[initials]	[sci_id] total_token_count	diff_token_count	comp_token_count	strong_token_count	weak_token_count	diff_score	nuance_score	comp_score).


## Other notes:
- Program is not documented, however functions, names, types, entries are clearly labelled. 
- Only possible user intervention codes are on line 103 and 104