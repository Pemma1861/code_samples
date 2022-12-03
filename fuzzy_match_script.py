from tqdm import tqdm
import time
from dataninja import teradatatools, fuzzymatching
import pandas as pd

tqdm.pandas()

# instantiate fuzzy matching and Teradata classes
sm = fuzzymatching.StringMatch()
td = teradatatools.TeradataDatabase()

# specify file paths and column names
NTOP_MATCHES = 5

base_file_path = \
    '/fuzzy_matches/adp_scrape_Q12020_ashlyn'

results_file_name = \
    'results.csv'

to_match_file_path = \
    f'{base_file_path}/adp_matches_from_ashlyn.csv'

matching_base_file_path = \
    f'{base_file_path}/all_dnb_query_top_lives_1000.sql'

# define column names to be used for matching
col_to_be_matched = 'company'
base_match_col = 'N_ORG'

drop_duplicates_by = [col_to_be_matched]
# END USER INPUT BELOW THIS LINE


# TODO remove this function from script (dataprocessing.py?)
def input_file_to_df(input_file, conn=None, verbose=False, encoding=None):
    """takes file path, reads csv, excel or sql to dataframe

        Args:
             input_file (str): file path of file to read in as dataframe
             conn (TD conn object): instance of td connection object
                dataninja.teradatatools.TeradataDatabase()
             verbose (bool): if verbose is true, it will print out the time
                elapsed during file read. Default is False.
             encoding (str): encoding variable to feed forward to pd_read_csv
        Returns:
            pd.Dataframe

        takes a file as input, returns dataframe based on file type extracted
       from the file name. File types currently supported: sql query strings
       (.sql), csv files (.csv) and excel files (.xlsx)"""
    time_start = time.time()
    # parse file type from end of string
    try:
        file_type = input_file.split('.')[-1].strip().lower()
    except:
        raise ValueError(
            f"file {input_file} does not end in .sql, .csv or .xlsx"
        )

    if file_type not in ['csv', 'sql', 'xlsx', 'xls']:
        raise ValueError(f"file type {file_type} not yet supported")

    if file_type == 'csv':
        df = pd.read_csv(input_file, encoding=encoding)
    elif file_type in ['xls', 'xlsx']:
        df = pd.read_excel(input_file)
    elif file_type == 'sql':
        if conn is None:
            raise ValueError('no conn argument passed, can not excecute query '
                             + 'without one')
        # read sql query from file
        with open(input_file, 'r') as f:
            sql_query = f.read()
        # execute query
        df = conn.sql_to_dataframe(sql_query)
    else:
        df = None

    time_end = time.time()

    if verbose:
        num_rows = len(df)
        elapsed_time = time_end - time_start
        print(f'{num_rows} returned in {elapsed_time} seconds' +
              f' from {input_file}')
    return df


# create dataframes, either from sql or
print('creating data frames...')

to_be_matched_df = input_file_to_df(to_match_file_path,
                                    conn=td,
                                    verbose=True,
                                    # encoding="ISO-8859-1"
                                    )
matching_base_df = input_file_to_df(matching_base_file_path,
                                    conn=td,
                                    verbose=True)

# if the columns being compared have the same name, rename them in the df
if col_to_be_matched == base_match_col:
    base_match_col_original = base_match_col
    base_match_col = base_match_col + '_base'
    matching_base_df.rename(columns={base_match_col_original: base_match_col},
                            inplace=True
                            )

# clean column names for matching
col_to_be_matched_clean = f'{col_to_be_matched}_Clean'
col_to_be_matched_clean_ns = f'{col_to_be_matched}_Clean_No_Stop'
base_match_col_clean = f'{base_match_col}_Clean'
base_match_col_clean_ns = f'{base_match_col}_Clean_Clean_No_Stop'

stop_pattern = sm.create_stop_word_pattern()

print('Cleaning Names to be Matched...')
# remove rows where name to match on is null
to_be_matched_df = to_be_matched_df[
    ~to_be_matched_df[col_to_be_matched].isnull()
].reset_index(drop=True)
# pre-process columns
to_be_matched_df[col_to_be_matched_clean] = \
    sm.prepare_df_columns(to_be_matched_df, col_to_be_matched)
# remove stopwords
to_be_matched_df[col_to_be_matched_clean_ns] = \
    sm.remove_stopwords(to_be_matched_df,
                        col_to_be_matched_clean,
                        stop_pattern)
# remove rows where matching column is null
to_be_matched_df = to_be_matched_df[
    ~to_be_matched_df[col_to_be_matched].isnull()
    & ~to_be_matched_df[col_to_be_matched_clean].isnull()
    & ~to_be_matched_df[col_to_be_matched_clean_ns].isnull()
].reset_index(drop=True)


print('Cleaning Matching Names Base df...')
# remove rows where name to match on is null
matching_base_df = matching_base_df[
    ~matching_base_df[base_match_col].isnull()
].reset_index(drop=True)
# pre-process columns
matching_base_df[base_match_col_clean] = \
    sm.prepare_df_columns(matching_base_df, base_match_col)
# remove stopwords
matching_base_df[base_match_col_clean_ns] = \
    sm.remove_stopwords(matching_base_df,
                        base_match_col_clean,
                        stop_pattern)
# remove rows where col to match against is null
matching_base_df = matching_base_df[
    ~matching_base_df[base_match_col].isnull()
    & ~matching_base_df[base_match_col_clean].isnull()
    & ~matching_base_df[base_match_col_clean_ns].isnull()
    ].reset_index(drop=True)

# create
clean_org_names = to_be_matched_df.copy()
org_name_clean = clean_org_names[col_to_be_matched_clean]

matching_base_df.reset_index(drop=True, inplace=True)
unique_base_names = matching_base_df[base_match_col_clean]


# find top matches with cosine similarity
num_compares = len(org_name_clean) * len(unique_base_names)
print(f'calculating cosine sim {num_compares} comparisons...')

sm.source_names = org_name_clean.tolist()
sm.target_names = unique_base_names.tolist()
sm.tokenize()
cosine_sim_df = sm.match(ntop=NTOP_MATCHES).reset_index()

# merge in columns from original dataframes)
cosine_sim_orig_columns_df = (
    cosine_sim_df
    .merge(to_be_matched_df,
           how='left',
           left_on='to_match_idx',
           suffixes=('', '_target'),
           right_index=True)
    .merge(matching_base_df,
           how='left',
           left_on='base_str_idx',
           suffixes=('', '_gndtruth'),
           right_index=True)
)

cosine_sim_orig_columns_df.to_csv(f'{base_file_path}/top_n_cosin_sim.csv',
                                  index=False)

print(
    'performing string similarity calculations...'
)

temp_strsim_score_df = sm.execute_string_sim_tests(
    cosine_sim_orig_columns_df,
    col_to_be_matched_clean_ns,
    base_match_col_clean_ns,
    col_to_be_matched_clean,
    base_match_col_clean
)

temp_strsim_score_df['max_strsim'] = (temp_strsim_score_df
                                      .groupby(col_to_be_matched)['average']
                                      .transform(max))


def find_top_employer_match(df, employer_size_col, row_score_col,
                            max_score_by_name_col, company_name_col,
                            threshold_delta=0.05):
    """find top fuzzy match prioritized by employer size

    Args:
        df (pd.Dataframe): dataframe containing fuzzy match scores and
            employer size column
        employer_size_col (str): column name that contains employer size
        row_score_col (str): column name containing individual fuzzy match
            score
        max_score_by_name_col (str): column containing the max fuzzy match
            score for a given source name
        company_name_col (str): column name containing the company name
            to be matched in dataframe
        threshold_delta (float): score deviation from max string matchting
            score to consider when taking highest employee count

    Returns:
        pd.Dataframe: dataframe removing duplicate rows on source name,
            keeping the largest employer size within the specified delta
            of the max string match score."""

    scored_df = df.copy()
    # filter out matches that scores are less than the threshold from max
    scored_df['delta_from_max'] = \
        scored_df[max_score_by_name_col] - scored_df[row_score_col]
    scored_df_filt_delta = scored_df[
        scored_df['delta_from_max'].le(threshold_delta)
    ]

    # with matching thresholds filtered, keep highest employer size per org
    scored_filt_sorted = scored_df_filt_delta.sort_values(
        by=[company_name_col, employer_size_col],
        ascending=[False, False]
    )
    top_match_by_ee_size = scored_filt_sorted.drop_duplicates(
        subset=[company_name_col]
    )

    return top_match_by_ee_size


try:
    max_ee_size_df = find_top_employer_match(
        df=temp_strsim_score_df,
        employer_size_col='Q_EE_TOT',
        row_score_col='average',
        max_score_by_name_col='max_strsim',
        company_name_col=col_to_be_matched,
        threshold_delta=0.05
    )

    max_ee_match_results_fp = f'{base_file_path}/top_ee_{results_file_name}'
    max_ee_size_df.to_csv(max_ee_match_results_fp, index=False)
except Exception as e:
    print('could not execute max employer, make sure Q_EE_TOT is present', e)


temp_strsim_score_df.sort_values(
    by=['max_strsim', col_to_be_matched, 'average'],
    ascending=[False, False, False],
    inplace=True)

temp_strsim_score_df.to_csv(
    f'{base_file_path}/top_n_{results_file_name}',
    index=False
)

top_matching_score = temp_strsim_score_df.sort_values(
    by=drop_duplicates_by + ['average'],
    ascending=False,
)

top_matching_score = top_matching_score.drop_duplicates(
    drop_duplicates_by,
    keep='first'
)

top_matching_score.to_csv(
    f'{base_file_path}/{results_file_name}',
    index=False
)
