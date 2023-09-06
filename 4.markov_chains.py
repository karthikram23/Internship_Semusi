from pyspark.sql.types import StringType, ArrayType
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import psycopg2






# Define the parameters
get_params = {
    'project_directory': '/home/balukarthikram/semusi',
    'database_name': 'postgres',
    'data_gen_path': '/home/balukarthikram/semusi/attribution_data.csv',
    'raw_data_path': '/home/balukarthikram/semusi/raw/attribution_data.csv',
    'bronze_tbl_path': '/opt/spark/test/bronze',
    'gold_user_journey_tbl_path': '/opt/spark/test/gold_user_journey',  
    'gold_attribution_tbl_path': '/opt/spark/test/gold_attribution', 
    'gold_ad_spend_tbl_path': '/home/balukarthikram/semusi/gold_ad_spend',
}


spark.sql("use {}".format(postgres))

# Create a Spark session
spark = SparkSession.builder.appName("TransitionMatrix").getOrCreate()



def get_transition_array(path):
  '''
    This function takes as input a user journey (string) where each state transition is marked by a >. 
    The output is an array that has an entry for each individual state transition.
  '''
  state_transition_array = path.split(">")
  initial_state = state_transition_array[0]
  
  state_transitions = []
  for state in state_transition_array[1:]:
    state_transitions.append(initial_state.strip()+' > '+state.strip())
    initial_state =  state
  
  return state_transitions



spark.udf.register("get_transition_array", get_transition_array, ArrayType(StringType()))



sql_query = """
CREATE OR REPLACE TEMPORARY VIEW markov_state_transitions AS
SELECT path,
  explode(get_transition_array(path)) as transition,
  1 AS cnt
FROM gold_user_journey
"""

spark.sql(sql_query)

# Assuming you have created the markov_state_transitions view, you can query it as follows:
result = spark.sql("SELECT * FROM markov_state_transitions")

# Show the result (you can choose to collect and process the data as needed)
result.show()


spark.stop()




transition_matrix_query = """
    CREATE OR REPLACE TEMPORARY VIEW transition_matrix AS
    SELECT
        left_table.start_state,
        left_table.end_state,
        left_table.total_transitions / right_table.total_state_transitions_initiated_from_start_state AS transition_probability
    FROM
        (
            SELECT
                transition,
                sum(cnt) total_transitions,
                TRIM(SPLIT(transition, '>')[0]) start_state,
                TRIM(SPLIT(transition, '>')[1]) end_state
            FROM
                markov_state_transitions
            GROUP BY
                transition
            ORDER BY
                transition
        ) left_table
        JOIN (
            SELECT
                a.start_state,
                sum(a.cnt) total_state_transitions_initiated_from_start_state
            FROM
                (
                    SELECT
                        TRIM(SPLIT(transition, '>')[0]) start_state,
                        cnt
                    FROM
                        markov_state_transitions
                ) AS a
            GROUP BY
                a.start_state
        ) right_table ON left_table.start_state = right_table.start_state
    ORDER BY
        end_state DESC
"""

# Execute the SQL query to create the transition_matrix view
spark.sql(transition_matrix_query)

# You can now query the transition_matrix view as needed
result = spark.sql("SELECT * FROM transition_matrix")

# Show the result (or process it as needed)
result.show()

# Stop the Spark session when done
spark.stop()

result = spark.sql("""
    SELECT start_state, round(sum(transition_probability), 2) as transition_probability_sum 
    FROM transition_matrix
    GROUP BY start_state
""")

# Show the result (or process it as needed)
result.show()

# Stop the Spark session when done
spark.stop()


transition_matrix_pd = spark.table('transition_matrix').toPandas()
transition_matrix_pivot = transition_matrix_pd.pivot(index='start_state',columns='end_state',values='transition_probability')

plt.figure(figsize=(10,5))
sns.set(font_scale=1.4)
sns.heatmap(transition_matrix_pivot,cmap='Blues',vmax=0.25,annot=True)



def get_transition_probability_graph(removal_state = "null"):
  '''
  This function calculates a subset of the transition probability graph based on the state to exclude
      removal_state: channel that we want to exclude from our Transition Probability Matrix
  returns subset of the Transition Probability matrix as pandas Dataframe
  '''
  
  transition_probability_pandas_df = None
  
  # Get the transition probability graph without any states excluded if the removal_state is null
  if removal_state == "null":
    transition_probability_pandas_df = spark.sql('''select
        trim(start_state) as start_state,
        collect_list(end_state) as next_stages,
        collect_list(transition_probability) as next_stage_transition_probabilities
      from
        transition_matrix
      group by
        start_state''').toPandas()
    
  # Otherwise, get the transition probability graph with the specified channel excluded/removed
  else:
    transition_probability_pandas_df = spark.sql('''select
      sub1.start_state as start_state,
      collect_list(sub1.end_state) as next_stages,
      collect_list(transition_probability) as next_stage_transition_probabilities
      from
      (
        select
          trim(start_state) as start_state,
          case
            when end_state == \"'''+removal_state+'''\" then 'Null'
            else end_state
          end as end_state,
          transition_probability
        from
          transition_matrix
        where
          start_state != \"'''+removal_state+'''\"
      ) sub1 group by sub1.start_state''').toPandas()

  return transition_probability_pandas_df



transition_probability_pandas_df = get_transition_probability_graph()



transition_probability_pandas_df


def calculate_conversion_probability(transition_probability_pandas_df, calculated_state_conversion_probabilities, visited_states, current_state="Start"):
  '''
  This function calculates total conversion probability based on a subset of the transition probability graph
    transition_probability_pandas_df: This is a Dataframe that maps the current state to all probable next stages along with their transition probability
    removal_state: the channel that we want to exclude from our Transition Probability Matrix
    visited_states: set that keeps track of the states that have been visited thus far in our state transition graph.
    current_state: by default the start state for the state transition graph is Start state
  returns conversion probability of current state/channel 
  '''
 
  #If the customer journey ends with conversion return 1
  if current_state=="Conversion":
    return 1.0
  
  #If the customer journey ends without conversion, or if we land on the same state again, return 0.
  #Note: this step will mitigate looping on a state in the event that a customer path contains a transition from a channel to that same channel.
  elif (current_state=="Null") or (current_state in visited_states):
    return 0.0
  
  #Get the conversion probability of the state if its already calculated
  elif current_state in calculated_state_conversion_probabilities.keys():
    return calculated_state_conversion_probabilities[current_state]
  
  else:
  #Calculate the conversion probability of the new current state
    #Add current_state to visited_states
    visited_states.add(current_state)
    
    #Get all of the transition probabilities from the current state to all of the possible next states
    current_state_transition_df = transition_probability_pandas_df.loc[transition_probability_pandas_df.start_state==current_state]
    
    #Get the next states and the corresponding transition probabilities as a list.
    next_states = current_state_transition_df.next_stages.to_list()[0]
    next_states_transition_probab = current_state_transition_df.next_stage_transition_probabilities.to_list()[0]
    
    #This will hold the total conversion probability of each of the states that are candidates to be visited next from the current state.
    current_state_conversion_probability_arr = []
    
    #Call this function recursively until all states in next_states have been incorporated into the total conversion probability
    import copy
    #Loop over the list of next states and their transition probabilities recursively
    for next_state, next_state_tx_probability in zip(next_states, next_states_transition_probab):
      current_state_conversion_probability_arr.append(next_state_tx_probability * calculate_conversion_probability(transition_probability_pandas_df, calculated_state_conversion_probabilities, copy.deepcopy(visited_states), next_state))
    
    #Sum the total conversion probabilities we calculated above to get the conversion probability of the current state.
    #Add the conversion probability of the current state to our calculated_state_conversion_probabilities dictionary.
    calculated_state_conversion_probabilities[current_state] =  sum(current_state_conversion_probability_arr)
    
    #Return the calculated conversion probability of the current state.
    return calculated_state_conversion_probabilities[current_state]



total_conversion_probability = calculate_conversion_probability(transition_probability_pandas_df, {}, visited_states=set(), current_state="Start")


total_conversion_probability



removal_effect_per_channel = {}
for channel in transition_probability_pandas_df.start_state.to_list():
  if channel!="Start":
    transition_probability_subset_pandas_df = get_transition_probability_graph(removal_state=channel)
    new_conversion_probability =  calculate_conversion_probability(transition_probability_subset_pandas_df, {}, visited_states=set(), current_state="Start")
    removal_effect_per_channel[channel] = round(((total_conversion_probability-new_conversion_probability)/total_conversion_probability), 2)



conversion_attribution={}

for channel in removal_effect_per_channel.keys():
  conversion_attribution[channel] = round(removal_effect_per_channel[channel] / sum(removal_effect_per_channel.values()), 2)

channels = list(conversion_attribution.keys())
conversions = list(conversion_attribution.values())

conversion_pandas_df= pd.DataFrame({'attribution_model': 
                                    ['markov_chain' for _ in range(len(channels))], 
                                    'channel':channels, 
                                    'attribution_percent': conversions})



sparkDF=spark.createDataFrame(conversion_pandas_df) 
sparkDF.createOrReplaceTempView("markov_chain_attribution_update")



attribution_pd = spark.table('gold_attribution').toPandas()

sns.set(font_scale=1.1)
sns.catplot(x='channel',y='attribution_percent',hue='attribution_model',data=attribution_pd, kind='bar', aspect=2)

