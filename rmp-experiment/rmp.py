import pandas as pd
import requests
from tqdm import tqdm

STORAGE_DIR = "/Users/shishiraravindan/Documents/work-RA/dess/storage"
FILE_NAME = "RMP-experiment.parquet"
RMP_GRAPHQL = "https://www.ratemyprofessors.com/graphql"
AUTH_TOKEN = "dGVzdDp0ZXN0"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Authorization": f"Basic {AUTH_TOKEN}"
}
SAMPLE_FILE = f"{STORAGE_DIR}/{FILE_NAME}"

AUTOCOMPLETE_SCHOOL_QUERY = """
query AutocompleteSearchQuery($query: String!) {
  autocomplete(query: $query) {
    schools {
      edges {
        node {
          id
          name
          city
          state
        }
      }
    }
  }
}
"""

SEARCH_TEACHER_QUERY = """
query NewSearchTeachersQuery($text: String!, $schoolID: ID!) {
  newSearch {
    teachers(query: {text: $text, schoolID: $schoolID}) {
      edges {
        node {
          id
          firstName
          lastName
          school {
            name
            id
          }
        }
      }
    }
  }
}
"""

GET_TEACHER_QUERY = """
query TeacherRatingsPageQuery($id: ID!) {
  node(id: $id) {
    ... on Teacher {
      id
      firstName
      lastName
      department
    }
  }
}
"""

def find_school_id(school_name):
    payload = {
        "query": AUTOCOMPLETE_SCHOOL_QUERY,
        "variables": {"query": school_name}
    }
    try:
        r = requests.post(RMP_GRAPHQL, json=payload, headers=HEADERS)
        data = r.json()
        schools = data.get("data", {}).get("autocomplete", {}).get("schools", {}).get("edges", [])
        for edge in schools:
            node = edge.get("node", {})
            if school_name.lower() in node.get("name", "").lower():
                return node.get("id")
        print(f"No matching school for '{school_name}' in results: {schools}")
        return None
    except Exception as e:
        print(f"Exception in find_school_id for '{school_name}': {e}")
        return None

def find_professor_id(first, last, school_id):
    payload = {
        "query": SEARCH_TEACHER_QUERY,
        "variables": {"text": f"{first} {last}", "schoolID": school_id}
    }
    try:
        r = requests.post(RMP_GRAPHQL, json=payload, headers=HEADERS)
        data = r.json()
        teachers = data.get("data", {}).get("newSearch", {}).get("teachers", {}).get("edges", [])
        for edge in teachers:
            node = edge.get("node", {})
            if node.get("firstName", "").lower() == first.lower() and node.get("lastName", "").lower() == last.lower():
                return node.get("id")
        print(f"No matching teacher for '{first} {last}' at school {school_id} in results: {teachers}")
        return None
    except Exception as e:
        print(f"Exception in find_professor_id for '{first} {last}' at school {school_id}: {e}")
        return None

def get_professor_department(prof_id):
    payload = {
        "query": GET_TEACHER_QUERY,
        "variables": {"id": prof_id}
    }
    try:
        r = requests.post(RMP_GRAPHQL, json=payload, headers=HEADERS)
        data = r.json()
        node = data.get("data", {}).get("node", {})
        return node.get("department")
    except Exception as e:
        print(f"Exception in get_professor_department for prof ID {prof_id}: {e}")
        return None

def get_rmp_department(row):
    try:
        school_id = find_school_id(row['university'])
        if not school_id:
            return None
        prof_id = find_professor_id(row['firstname'], row['lastname'], school_id)
        if not prof_id:
            return None
        return get_professor_department(prof_id)
    except Exception as e:
        print(f"Error for {row['firstname']} {row['lastname']} at {row['university']}: {e}")
        return None
    

if __name__ == "__main__":
    print(find_school_id("Stanford University"))

    # df = pd.read_parquet(SAMPLE_FILE)
    # df_sample = df.sample(10)
    # tqdm.pandas()
    # df_sample['department_rmp'] = df_sample.progress_apply(get_rmp_department, axis=1)
    # # df_sample.to_parquet(f"{STORAGE_DIR}/RMP-experiment-with-department.parquet")
    # print(df_sample)