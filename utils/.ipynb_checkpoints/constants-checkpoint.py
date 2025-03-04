DATA_RAW = 'data'
DATA_PRO = 'data/processed'

# names of datasets
QC23_raw = 'QC23_Dataset'
QC23_clean = 'cleaned_QC23_Dataset'
QC23_prob = 'qc23_prob'
QC23_log = 'qc23_log'
QC23_prob_assis_map = 'qc23_prob_assis_map'

# names of file types
CSV = '.csv'
JSON = '.json'

# names of columns
PROBLEM_LOG_ID = 'problem_log_id'
STUDENT_USER_ID = 'student_user_id'
ASSIGNMENT_ID = 'assignment_id'
ASSISTMENT_ID = 'assistment_id'
PROBLEM_ID = 'problem_id'
SKILL_CODE = 'skill_code'
SKILL_NAME = 'skill_name'
PROBLEM_TYPE_ID = 'problem_type_id'
PROBLEM_TYPE = 'problem_type'
PROBLEM_ORDER = 'problem_order'
PROBLEM_BODY = 'problem_body'
ANSWER_TEXT = 'answer_text'
CORRECTNESS = 'correctness'

# Dtypes:
EXPECTED_DTYPES = {'problem_log_id': 'int64',
        'student_user_id': 'int64',
        'prior_problems_count': 'int64',
        'prior_pr10_avg_correctness': 'float64',
        'prior_5pr_avg_correctness': 'float64',
        'prior_pr_avg_correctness': 'float64',
        'problem_set_id': 'int64',
        'student_class_id': 'int64',
        'teacher_id': 'int64',
        'assignment_id': 'int64',
        'assignment_start_time': 'object',
        'assignment_end_time': 'object',
        'assignment_completion': 'int64',
        'assistment_id': 'int64',
        'problem_id': 'int64',
        'problem_set_name': 'object',
        'content_source': 'object',
        'is_flat_skill_builder': 'int64',
        'is_scaffold': 'int64',
        'problem_type_id': 'int64',
        'skill_code': 'object',
        'skill_name': 'object',
        'problem_type': 'object',
        'problem_body': 'object',
        'problem_order': 'int64',
        'problem_start_time': 'object',
        'problem_end_time': 'object',
        'answer_text': 'object',
        'correctness': 'float64',
        'hint_count': 'int64',
        'bottom_hint': 'bool',
        'first_action_is_attempt': 'float64'
        }
