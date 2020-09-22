# Cognitive Complexity 


## PDF: 
- [Link to File](/orsc.CEO cognitive complexity.pdf)

## Prerequisite:
- Recommended file name format: _author_names_.txt (with given reviews in each .txt file)
- 

## Input:
- Takes a directory of .txt 
- .txt must not contain any dataframes. Must be all texts

## Output:
- A dataframe with each .txt file name as a id/file name column 

## START: How to execute program 



## PURPOSE: How is it done - (per notes from CEO's Q&A calls):
- Our measure of differentiation language was the number of such words used divided by all of the [CEO's] Q&A words in a given call.
- Our measure of nuance leverages both valences: number of weak words divided by the total number of strong and weak words spoken by the [CEO's] in the Q&A part of the call. We added 0.01 to the numerator and 0.02 to the denominator to allow for meaningful values when either term was zero.
- Our final measure was the ratio of comparative words to all words spoken by the [CEO's] in the Q&A portion of a given conference call.
- Using this sample, we measured our three component indicators of [CEO's] cognitive complexity (differentiation, nuance, and comparison) for each call.
Cognitive scores are located at the last 3 columns of the pew_dataset, with the token count (word count) being placed in the front of the scores. Everything after column txt-wordcount, are newly added (scientists_full_file_name	file_name_id	name	initials	sci_id	total_token_count	diff_token_count	comp_token_count	strong_token_count	weak_token_count	diff_score	nuance_score	comp_score) . We might not need the file_name_id, name, initials, sci_id