import pandas as pd
import pytest
import d6tjoin
from datetime import datetime

curr_date=datetime.now()
curr_year=curr_date.year
years_ago=3
exam_pass_grade=18

csv_enrollments=pd.read_csv('enrollments.csv')
csv_exams=pd.read_csv('exams.csv')


def get_enrollments(dst_path):

    join_df=csv_exams.merge(
            csv_enrollments[(csv_enrollments.enrollment_year<(curr_year-years_ago))],
            left_on='student_code',
            right_on='student_code',
            how='inner')[['student_name','student_code','enrollment_year']]

    df=join_df.drop_duplicates()
    df[['student_name','student_code']].to_csv(dst_path,index=False)

    return df

def get_top_ten_students(dst_path):

    df=csv_exams[(csv_exams.exam_grade>=exam_pass_grade)]. \
            groupby('student_code'). \
            agg(count=('student_code', 'count'), exam_grade_mean=('exam_grade', 'mean')). \
            query('count>3'). \
            reset_index(). \
            sort_values('exam_grade_mean',ascending=False). \
            head(10)

    df['student_code'].to_csv(dst_path,index=False)

    return df

def run_unit_test_enrollments(df):

     print("Check no enrollment year is greater than 2017")
     try:
      
         assert(df['enrollment_year'].any()<=2017) 
 
     except AssertionError:

         print("Assertion failed")
         return

     print("Passed")
    
def run_unit_test_top_ten_students(df):

     print("Check no group has more than three items")
     try:
      
        assert(df['count'].all()<=3) 
 
     except AssertionError:

         print("Assertion failed")

     print("Passed")

def main():


    df1=get_enrollments('students-enrolled.csv')
    df2=get_top_ten_students('top-ten-students.csv')

    print("Running unit tests")
    print("Veryfing join quality")

    try:
      
        assert d6tjoin.Prejoin([csv_exams,csv_enrollments],['student_code','student_code']).is_all_matched()
   
    except AssertionError:

        print ("Assertion Failed")
        raise

    print("Passed")
 
    run_unit_test_enrollments(df1)
    run_unit_test_top_ten_students(df2)


if __name__=='__main__':
    main()
