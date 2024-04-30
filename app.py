from flask import Flask, render_template, request, jsonify, render_template, send_file, Response, session, redirect, url_for
from itertools import combinations, permutations, product
from pymongo import MongoClient, ASCENDING
from collections import Counter
from flask_wtf import FlaskForm
from bson import json_util
from gridfs import GridFS
import pandas as pd
import tempfile
import random
import json
import csv
import os
import io
import re



app = Flask(__name__)
app.config['SECRET_KEY'] = 'dhyani22052000'





# ////////////////////////////////////////////////////////
# initial route
@app.route('/', methods=['GET', 'POST'])
def main():
    return render_template("index.html")





# ////////////////////////////////////////////////////////
# MongoDB connection settings
client = MongoClient("mongodb://localhost:27017/")  # Replace with your MongoDB connection string
db = client["database10"]  # Replace with your MongoDB database name





# ////////////////////////////////////////////////////////
# view collections
@app.route('/collection/<collection_name>')
def view_collection(collection_name):
    collection = db[collection_name]
    documents = list(collection.find({}))
    return render_template('doc.html', collection_name=collection_name, documents=documents)





global_o_weighted_sscore = None
global_o_weighted_strucscore = None
global_metrics = []
global_entity_metrics = {}
# ////////////////////////////////////////////////////////
# our algorithm
@app.route('/access_load', methods=['POST', 'GET'])


def mul_query():

    global global_o_weighted_sscore
    global global_o_weighted_strucscore
    global global_metrics
    global global_entity_metrics

    access_path_file = request.files['query-text'].filename
    entity_attr_file = request.files['entity-attr'].filename
    custom_schema_file = request.files['custom_schema'].filename

    result = find_final_collection(access_path_file)
    dag_for_algo1, dag_for_algo2, queries, entities_list = result[1:]
    collection=db.list_collection_names()


    new_path1 = generate_indirect_paths(dag_for_algo1)
    new_path2 = generate_indirect_paths2(dag_for_algo2)

    for entity in entities_list:
        if entity not in '/'.join(dag_for_algo1).split('/'):
            new_path1.append(entity)
        if entity not in ('*'.join(dag_for_algo2).split('*') + '^'.join(dag_for_algo2).split('^')):
            new_path2.append(entity)
    
    schema_metrics = {}
    schema_metrics_list = []
    path_scores = []
    dir_edge_scores = []
    all_edge_scores = []
    required_collections = []

    for query in queries:

        ps = pathScore(query, new_path1)
        des = dirEdgeScore(query, dag_for_algo1)
        aes = allEdgeScore(query, dag_for_algo1)
        r = req_col(query, new_path1)

        query = query.replace("\n", "")
        query_exists = any(item[0] == query for item in schema_metrics_list)
        
        if not query_exists:
            schema_metrics_list.append([query, ps, des, aes, r])

        path_scores.append(ps)
        dir_edge_scores.append(des)
        all_edge_scores.append(aes)
        required_collections.append(r)


    s_score_path = sum(row[1] for row in schema_metrics_list) / len(schema_metrics_list)
    s_score_dir_edge = sum(row[2] for row in schema_metrics_list) / len(schema_metrics_list)
    s_score_all_edge = sum(row[3] for row in schema_metrics_list) / len(schema_metrics_list)
    s_score_req_cols = len(schema_metrics_list) / sum(row[4] for row in schema_metrics_list)

    o_weighted_sscore = round(0.35*round(s_score_path, 2) + 0.30*round(s_score_dir_edge, 2) + 0.25*round(s_score_all_edge, 2) + 0.10*round(s_score_req_cols, 2), 2)
    global_o_weighted_sscore = o_weighted_sscore


    # Append the SScore row to schema_metrics_list
    schema_metrics_list.append(["SScore", round(s_score_path, 2), round(s_score_dir_edge, 2), round(s_score_all_edge, 2), round(s_score_req_cols, 2)])
    # schema_metrics_list.append(["Weighted Score", round(0.35*round(s_score_path, 2) + 0.30*round(s_score_dir_edge, 2) + 0.25*round(s_score_all_edge, 2) + 0.10*round(s_score_req_cols, 2), 2)])
    schema_metrics_list.append(["Weighted Score", o_weighted_sscore])
    global_metrics = schema_metrics_list


    ce = colExistence(new_path2, entities_list)
    gd = globalDepth(new_path2)
    dtc = docTypeCopies(new_path2)
    rl = refLoad(new_path2)
    dcis = docCopiesInCol(new_path2)

    o_weighted_strucscore = round((0.25*ce + 0.30*gd + 0.20*dtc + 0.15*rl + 0.10*dcis), 2)
    global_o_weighted_strucscore = o_weighted_strucscore

    entity_metrics = {
        'No. of Collections': ce,
        'Global Depth': gd,
        'No. of Documents': dtc,
        'Reference Load': rl,
        'No. of Document Copies': dcis,
        'Weighted Score': o_weighted_strucscore
    }

    global_entity_metrics = entity_metrics
    print(global_entity_metrics)

    schema_metrics = {
        'metrics': schema_metrics_list,
        'entity_metrics': entity_metrics,
        'paths': new_path1,
        'paths2': new_path2,
        'weighted_sscore': o_weighted_sscore,
        'weighted_strucscore': o_weighted_strucscore
    }


    return render_template('collections3.html', collections=collection, access_path_file=access_path_file, entity_attr_file=entity_attr_file, custom_schema_file=custom_schema_file, queries=queries, entities_list=entities_list, schema_metrics=schema_metrics, entity_metrics=entity_metrics, paths1=new_path1, paths2=new_path2)
    # return render_template('collections.html', collections=collection, access_path_file=access_path_file, entity_attr_file=entity_attr_file, result=result, queries=queries, entities_list=entities_list, schema_metrics=schema_metrics)




def find_final_collection(access_path_file):
    collection_documents = {}
    entity_attr_file = request.files['entity-attr']
    cardinality_file = request.files['cardinality']

    dag_for_algo1 = []
    dag_for_algo2 = []
    
    collection_names = db.list_collection_names()
    for collection_name in collection_names:
        db[collection_name].drop()

    entity_attributes = {}
    split_lines = []
    lists_dict = {}
    attr_val_pairs = {}

    with open(access_path_file, 'r') as file:
        for i, line in enumerate(file, start=1):
            values = line.strip().split('/')
            split_lines.append(values)
            list_name = f"list_{i}"
            lists_dict[list_name] = values

            attr = ""
            val = ""
            last_ele = values[-1]  # Extract the last part after splitting by '/'




            if '?' in last_ele:
                # print(last_ele)
                last_parts = last_ele.split('?')
                if '=' in last_parts[1]:
                    attr = last_parts[1].split('=')[0]
                    if type(val) == 'int':
                        val = int(last_parts[1].split('=')[1])
                    else:
                        val = last_parts[1].split('=')[1]
                elif '>' in last_parts[1]:
                    attr = last_parts[1].split('>')[0]
                    if type(val) == 'int':
                        val = int(last_parts[1].split('>')[1])
                    else:
                        val = last_parts[1].split('>')[1]
                elif '<' in last_parts[1]:
                    attr = last_parts[1].split('<')[0]
                    if type(val) == 'int':
                        val = int(last_parts[1].split('<')[1])
                    else:
                        val = last_parts[1].split('<')[1]
                elif '>=' in last_parts[1]:
                    attr = last_parts[1].split('>=')[0]
                    if type(val) == 'int':
                        val = int(last_parts[1].split('>=')[1])
                    else:
                        val = last_parts[1].split('>=')[1]
                elif '<=' in last_parts[1]:
                    attr = last_parts[1].split('<=')[0]
                    if type(val) == 'int':
                        val = int(last_parts[1].split('<=')[1])
                    else:
                        val = last_parts[1].split('<=')[1]
                elif '!=' in last_parts[1]:
                    attr = last_parts[1].split('!=')[0]
                    if type(val) == 'int':
                        val = int(last_parts[1].split('!=')[1])
                    else:
                        val = last_parts[1].split('!=')[1]
                else:
                    attr = last_parts[1]
                values[-1] = last_parts[0]  # Replace the last element with the value before the '?'
                
                if attr is not None and val is not None:
                    db[values[-1]].create_index([(attr, ASCENDING)])
                    query = {attr: val}
                    results = db[values[-1]].find(query)
                    attr_val_pairs[values[-1]] = {attr: val}
                elif attr is not None and val is None:
                    db[values[-1]].create_index([(attr, ASCENDING)])
                    results = db[values[-1]].find({attr: {"$exists": True}})
                    attr_val_pairs[values[-1]] = {attr: {"$exists": True}}
    num_lines = len(lists_dict)



    with open(entity_attr_file.filename, 'r') as entity_attr_csv_file:
        csv_reader = csv.DictReader(entity_attr_csv_file)

        for s_row in csv_reader:
            entity_name, attribute_name, data_type, *_ = s_row
            entity_name = s_row['EntityName']
            attribute_name = s_row['AttributeName']
            data_type = s_row['DataType']

            # Check if the entity already exists in the dictionary
            if entity_name in entity_attributes:
                entity_attributes[entity_name].append({attribute_name: data_type})
            else:
                entity_attributes[entity_name] = [{attribute_name: data_type}] # Create a new entry for the entity



    # Process cardinality_file using pandas
    df = pd.read_csv(cardinality_file)
    with open(cardinality_file.filename, newline='') as cardfile:
        csv_reader = csv.reader(cardfile)
        next(csv_reader)  # Skip the header row

        for c_row in csv_reader:
            entity1, entity2, rel, card1, card2 = c_row

    embedded_to =[]
    embedding_coll = []
    index_later = {}

    for list_name, values in lists_dict.items():
        for i in range(len(values)-2, -1, -1):
            curr_element = values[i]
            next_element = values[i+1]
            # print(curr_element, next_element)
            matching_row = df[(df['Entity1'].str.lower() == curr_element) & (df['Entity2'].str.lower() == next_element)]
            rev_match = df[(df['Entity2'].str.lower() == curr_element) & (df['Entity1'].str.lower() == next_element)]


# ******************** Main Algorithm ********************
            
            if not matching_row.empty:

                card1_value = matching_row['Card1'].values[0]
                card2_value = matching_row['Card2'].values[0]

                if card1_value == 1 and card2_value > 500: # one-to-many as well as one-to-squillion
                    if {"ref_"+next_element+"_id": "array"} not in entity_attributes[curr_element]:
                        entity_attributes[curr_element].append({"ref_"+next_element+"_id": "array"})

                        dag_for_algo1.append(f"{curr_element}/{next_element}") # append to dag_for_algo list
                        dag_for_algo2.append(f"{curr_element}*{next_element}") # append to dag_for_algo list

                elif card1_value == 1 and card2_value <= 500:
                    if {next_element: entity_attributes[next_element]} not in entity_attributes[curr_element]:
                        entity_attributes[curr_element].append({next_element: entity_attributes[next_element]})

                        dag_for_algo1.append(f"{curr_element}/{next_element}") # append to dag_for_algo list
                        dag_for_algo2.append(f"{curr_element}^{next_element}") # append to dag_for_algo list

                        db[next_element].delete_many({})
                        embedded_to.append(curr_element)
                        embedding_coll.append(next_element)
                        if next_element in attr_val_pairs:
                            index_later[curr_element] = {next_element: attr_val_pairs[next_element]}

                elif card1_value > 1 and card2_value == 1:
                    if {"ref_"+curr_element+"_id": "array"} not in entity_attributes[next_element]:
                        entity_attributes[next_element].append({"ref_"+curr_element+"_id": "array"})

                        dag_for_algo1.append(f"{next_element}/{curr_element}") # append to dag_for_algo list
                        dag_for_algo2.append(f"{curr_element}*{next_element}") # append to dag_for_algo list


                # elif card1_value <= 500 and card2_value == 1:
                #     if {curr_element: entity_attributes[curr_element]} not in entity_attributes[next_element]:
                #         entity_attributes[next_element].append({curr_element: entity_attributes[curr_element]})

                #         dag_for_algo1.append(f"{next_element}/{curr_element}") # append to dag_for_algo list
                #         dag_for_algo2.append(f"{next_element}*{curr_element}") # append to dag_for_algo list


                # elif card1_value > 500 and card2_value == 1:
                #     if {"ref_"+curr_element+"_id": "array"} not in entity_attributes[next_element]:
                #         entity_attributes[next_element].append({"ref_"+curr_element+"_id": "array"})

                #         dag_for_algo1.append(f"{next_element}/{curr_element}") # append to dag_for_algo list
                #         dag_for_algo2.append(f"{next_element}*{curr_element}") # append to dag_for_algo list
                

                elif card1_value > 1 and card2_value > 1:
                    if {"ref_"+next_element+"_id": "array"} not in entity_attributes[curr_element]:
                        entity_attributes[curr_element].append({"ref_"+next_element+"_id": "array"})
                        
                        dag_for_algo1.append(f"{curr_element}/{next_element}") # append to dag_for_algo list
                        dag_for_algo2.append(f"{curr_element}*{next_element}") # append to dag_for_algo list
                    
                    if {"ref_"+curr_element+"_id": "array"} not in entity_attributes[next_element]:
                        entity_attributes[next_element].append({"ref_"+curr_element+"_id": "array"})

                        dag_for_algo1.append(f"{next_element}/{curr_element}") # append to dag_for_algo list
                        dag_for_algo2.append(f"{next_element}*{curr_element}") # append to dag_for_algo list


            if not rev_match.empty:

                card1_value = rev_match['Card2'].values[0]
                card2_value = rev_match['Card1'].values[0]

                if card1_value == 1 and card2_value > 500: # one-to-many as well as one-to-squillion
                    if {"ref_"+next_element+"_id": "array"} not in entity_attributes[curr_element]:
                        entity_attributes[curr_element].append({"ref_"+next_element+"_id": "array"})

                        dag_for_algo1.append(f"{curr_element}/{next_element}") # append to dag_for_algo list
                        dag_for_algo2.append(f"{curr_element}*{next_element}") # append to dag_for_algo list

                elif card1_value == 1 and card2_value <= 500: # one-to-one
                    if {next_element: entity_attributes[next_element]} not in entity_attributes[curr_element]:
                        entity_attributes[curr_element].append({next_element: entity_attributes[next_element]})

                        dag_for_algo1.append(f"{curr_element}/{next_element}") # append to dag_for_algo list
                        dag_for_algo2.append(f"{curr_element}^{next_element}") # append to dag_for_algo list

                        db[next_element].delete_many({})
                        embedded_to.append(curr_element)
                        embedding_coll.append(next_element)
                        if next_element in attr_val_pairs:
                            index_later[curr_element] = {next_element: attr_val_pairs[next_element]}


                elif card1_value > 1 and card2_value == 1:
                    if {"ref_"+curr_element+"_id": "array"} not in entity_attributes[next_element]:
                        entity_attributes[next_element].append({"ref_"+curr_element+"_id": "array"})

                        dag_for_algo1.append(f"{next_element}/{curr_element}") # append to dag_for_algo list
                        dag_for_algo2.append(f"{next_element}*{curr_element}") # append to dag_for_algo list


                # elif card1_value <= 500 and card2_value == 1:
                #     if {curr_element: entity_attributes[curr_element]} not in entity_attributes[next_element]:
                #         entity_attributes[next_element].append({curr_element: entity_attributes[curr_element]})

                #         dag_for_algo1.append(f"{next_element}/{curr_element}") # append to dag_for_algo list
                #         dag_for_algo2.append(f"{next_element}*{curr_element}") # append to dag_for_algo list

                
                # elif card1_value > 500 and card2_value == 1:
                #     if {"ref_"+curr_element+"_id": "array"} not in entity_attributes[next_element]:
                #         entity_attributes[next_element].append({"ref_"+curr_element+"_id": "array"})

                #         dag_for_algo1.append(f"{next_element}/{curr_element}") # append to dag_for_algo list
                #         dag_for_algo2.append(f"{next_element}*{curr_element}") # append to dag_for_algo list


                elif card1_value > 1 and card2_value > 1:
                    if {"ref_"+next_element+"_id": "array"} not in entity_attributes[curr_element]:
                        entity_attributes[curr_element].append({"ref_"+next_element+"_id": "array"})
                        
                        dag_for_algo1.append(f"{curr_element}/{next_element}") # append to dag_for_algo list
                        dag_for_algo2.append(f"{curr_element}*{next_element}") # append to dag_for_algo list


                    if {"ref_"+curr_element+"_id": "array"} not in entity_attributes[next_element]:
                        entity_attributes[next_element].append({"ref_"+curr_element+"_id": "array"})

                        dag_for_algo1.append(f"{next_element}/{curr_element}") # append to dag_for_algo list
                        dag_for_algo2.append(f"{next_element}*{curr_element}") # append to dag_for_algo list



    for entity_name, attributes in entity_attributes.items():
            collection = db[entity_name]
            
            # Combine all inner dictionaries into a single document
            combined_document = {}
            for inner_dict in attributes:
                combined_document.update(inner_dict)
            
            # Insert the combined document into the collection
            collection.insert_one(combined_document)



    if index_later != {}:
        for entity_name, attributes in index_later.items():
            for attr, val in attributes.items():
                for ind_attr, ind_val in val.items():
                    if ind_attr is not None and ind_val is not None:
                        db[entity_name].create_index([(ind_attr, ASCENDING)])
                        query = {ind_attr: ind_val}
                        results = db[entity_name].find(query)
                    elif ind_attr is not None and val is None:
                        db[entity_name].create_index([(ind_attr, ASCENDING)])
                        results = db[entity_name].find({ind_attr: {"$exists": True}})


# ******************** Generate a txt file that contains the generated schema structure ********************
    collection_data = {}
    file_name = "mongodb_data.txt"
    with open(file_name, "w") as file:
        pass
    for collection_name in db.list_collection_names():
        collection = db[collection_name]
        documents = collection.find()

        with open(file_name, "a") as text_file:
            for doc in documents:
                text_file.write(collection_name + ":\n\n")
                for field in doc:
                    text_file.write(str(field) + ":" + str(doc[field]) + "\n")
                text_file.write("\n\n")


    queries = []
    with open(access_path_file, 'r') as file:
        for line in file:
            if str("\n") in line:
                queries.append(line[:-1])  # Remove '\n' from end of string
                queries.append(line)
            else:
                queries.append(line)

    entities_list = []
    # Read the files and do evaluation
    with open(entity_attr_file.filename, 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        next(csv_reader)
        for row in csv_reader:
            # Access the 'EntityName' column (assuming it's the first column)
            entity_name = row[0]
            # Process the entity_name here
            if entity_name not in entities_list:
                entities_list.append(entity_name)


    return send_file("mongodb_data.txt", as_attachment=True), dag_for_algo1, dag_for_algo2, queries, entities_list







# ////////////////////////////////////////////////////////
# evaluation functions 1
@app.route('/evaluate', methods=['POST', 'GET'])
def calculate_schema_metrics():

    global global_o_weighted_sscore
    global global_o_weighted_strucscore
    global global_metrics
    global global_entity_metrics

    access_path_file = request.form['access_path_file']

    queries = []
    with open(access_path_file, 'r') as file:
        for line in file:
            if str("\n") in line:
                queries.append(line[:-1])  # Remove '\n' from end of string
                queries.append(line)
            else:
                queries.append(line)



    entity_attr_file = request.form['entity_attr_file']

    # Read the files and do evaluation
    with open(entity_attr_file, 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        entities_list = []
        next(csv_reader)
        for row in csv_reader:
            # Access the 'EntityName' column (assuming it's the first column)
            entity_name = row[0]
            # Process the entity_name here
            if entity_name not in entities_list:
                entities_list.append(entity_name)



    custom_schema_file = request.form['custom_schema_file']

    schemas = []
    with open(custom_schema_file, 'r') as file:
        for line in file:
            # 
            line = line.strip()  # Remove leading and trailing whitespace, including '\n'
            if line:  # Check if the line is not empty after stripping
                schemas.append(line)

    # schemas = ['user references order; user references product; product references user; cart embeds product; order embeds order_item', 'user references order; user references product; product references user; cart references product; order references order_item']


    schema_metrics = {}
    entity_metrics = {}
    
    for idx, schema in enumerate(schemas):
        # Initialize a list to hold metrics for each query in this schema
        schema_metrics_list = []
        entity_metrics_list = []

        # Generate DAG
        dag_entities, dag_paths, all_edges = generate_dag(schema)
        paths = generate_indirect_paths(dag_paths)
        dag_entities2, dag_paths2, all_edges2 = generate_dag2(schema)
        paths2 = generate_indirect_paths2(dag_paths2)
        # for e in all_edges:
        #     print(e)

        # Calculate metrics for each query
        for query in queries:
            
            count = req_col(query, paths)
            dir_edge_score = dirEdgeScore(query, dag_paths)
            all_edge_score = allEdgeScore(query, all_edges)
            path_score = pathScore(query, paths)
            
            query = query.replace("\n", "")
            query_exists = any(item[0] == query for item in schema_metrics_list)
            
            if not query_exists:
                schema_metrics_list.append([query, path_score, dir_edge_score, all_edge_score, count])

        # Calculate the SScore and append it as a new row to schema_metrics_list
        s_score_path = sum(row[1] for row in schema_metrics_list) / len(schema_metrics_list)
        s_score_dir_edge = sum(row[2] for row in schema_metrics_list) / len(schema_metrics_list)
        s_score_all_edge = sum(row[3] for row in schema_metrics_list) / len(schema_metrics_list)
        # s_score_req_cols = sum(row[4] for row in schema_metrics_list) / len(schema_metrics_list)
        s_score_req_cols = len(schema_metrics_list) / sum(row[4] for row in schema_metrics_list)

        weighted_sscore = round(0.35*round(s_score_path, 2) + 0.30*round(s_score_dir_edge, 2) + 0.25*round(s_score_all_edge, 2) + 0.10*round(s_score_req_cols, 2), 2)

        # Append the SScore row to schema_metrics_list
        schema_metrics_list.append(["SScore", round(s_score_path, 2), round(s_score_dir_edge, 2), round(s_score_all_edge, 2), round(s_score_req_cols, 2)])
        schema_metrics_list.append(["Weighted Score", weighted_sscore])


        ce = colExistence(paths2, entities_list)
        gd = globalDepth(paths2)
        dtc = docTypeCopies(paths2)
        rl = refLoad(paths2)
        dcis = docCopiesInCol(paths2)
        weighted_strucscore = round((0.25 * ce + 0.30 * gd + 0.20 * dtc + 0.15 * rl + 0.10 * dcis), 2)

        entity_metrics = {
            'No. of Collections': ce,
            'Global Depth': gd,
            'No. of Documents': dtc,
            'Reference Load': rl,
            'No. of Document Copies': dcis,
            'Weighted Score': weighted_strucscore
        }

        schema_metrics[schema] = schema_metrics_list
        schema_metrics[schema] = {
            'metrics': schema_metrics_list,
            'entity_metrics': entity_metrics,
            'paths': paths,
            'paths2': paths2,
            'num': idx + 1,
            'weighted_sscore': weighted_sscore,
            'weighted_strucscore': weighted_strucscore
        }

    # schema_metrics['Our Schema'] = global_metrics
    # schema_metrics['Our Schema'] = {
    #         'metrics': global_metrics,
    #         'entity_metrics': global_entity_metrics,
    #         'num': 'Our Schema',
    #         'weighted_sscore': global_o_weighted_sscore,
    #         'weighted_strucscore': global_o_weighted_strucscore
    #     }
    # print(schema_metrics)
    # Create schema ranking based on SScore and Weighted Score
    schema_ranking = sorted(schema_metrics.items(), key=lambda x: (x[1]['metrics'][0][1], -x[1]['entity_metrics']['Weighted Score']), reverse=True)
    
    # Prepare schema ranking with index for rendering in the template
    schema_ranking_with_index = [(rank + 1, "Schema "+str(schema_info['num']), schema_info['weighted_sscore'], schema_info['weighted_strucscore']) for rank, (schema, schema_info) in enumerate(schema_ranking)]


    return render_template("eval.html", schemas=schemas, paths=paths, paths2=paths2, schema_metrics=schema_metrics, entity_metrics=entity_metrics, entities_list=entities_list, schema_metrics_list=schema_metrics_list, schema_ranking=schema_ranking_with_index)


class Entity:
    def __init__(self, name):
        self.name = name
        self.children = []


def generate_dag(schema):
    relationships = schema.split(";")
    dag_entities = {}
    dag_paths = []
    all_edges = []

    # Parse relationships and create entities
    for rel in relationships:
        parts = rel.split()
        if len(parts) < 3:
            # Handle incomplete relationships
            # dag_paths.append(rel)
            # all_edges.append(rel)
            continue

        src = parts[0]
        rel_type = parts[1]
        dest = parts[2]

        if src not in dag_entities:
            dag_entities[src] = Entity(src)
        if dest not in dag_entities:
            dag_entities[dest] = Entity(dest)

        if rel_type == 'embeds':
            dag_entities[src].children.append(dag_entities[dest])
            dag_paths.append(f"{src}/{dest}")
            all_edges.append(f"{src}/{dest}")
            all_edges.append(f"{dest}/{src}")
        elif rel_type == 'references':
            dag_entities[src].children.append(dag_entities[dest])
            dag_paths.append(f"{src}/{dest}")
            all_edges.append(f"{src}/{dest}")
            all_edges.append(f"{dest}/{src}")

    return dag_entities, dag_paths, all_edges

def generate_indirect_paths(direct_edges):
    # Dictionary to store direct connections for quick lookup
    direct_map = {}
    for edge in direct_edges:
        source, dest = edge.split('/')
        if source not in direct_map:
            direct_map[source] = []
        direct_map[source].append((dest, '/'))
    
    # Function to perform DFS and collect all indirect paths
    def find_all_paths(current_node, current_path, visited, all_paths):
        if current_node in visited:
            return
        
        visited.add(current_node)
        if current_node in direct_map:
            for next_node, symbol in direct_map[current_node]:
                # Extend the current path with the next node and symbol
                new_path = current_path + [symbol, next_node]
                find_all_paths(next_node, new_path, visited, all_paths)
        
        # Add the completed current path to all_paths when we reach a leaf node
        all_paths.append(''.join(current_path))
        visited.remove(current_node)
    
    all_paths = []
    # Start DFS from each node in direct_map to collect all possible paths
    for start_node in direct_map:
        find_all_paths(start_node, [start_node], set(), all_paths)
    
    # Sort all_paths by length in descending order
    all_paths.sort(key=len, reverse=True)
    
    longest_paths = []
    seen_paths = set()  # Set to keep track of paths that are already added as longest_paths
    
    # Iterate over all_paths and add longest paths to the result
    for path in all_paths:
        if not any(path.startswith(existing_path) for existing_path in seen_paths):
            longest_paths.append(path)
            seen_paths.add(path)

    
    diction = get_lengths_dictionary(longest_paths)
    final = remove_substring_keys(diction)
    final_paths = list(final.keys())
    
    return final_paths


def get_lengths_dictionary(edges_list):
    lengths_dict = {}
    for edge in edges_list:
        lengths_dict[edge] = len(edge.split('/')) - 1
    
    sorted_dict = dict(sorted(lengths_dict.items(), key=lambda item: item[1], reverse=True))
    return sorted_dict
def remove_substring_keys(input_dict):

    # Create a list of keys to delete
    keys_to_delete = []

    # Iterate over each key in the dictionary
    for key in input_dict:
        # Check if any substring of the current key is also a key in the dictionary
        for other_key in input_dict:
            if key != other_key and key in other_key:
                # If a substring of 'key' exists in 'other_key', mark 'key' for deletion
                keys_to_delete.append(key)
                break  # No need to check further for this 'key'

    # Remove keys marked for deletion from the dictionary
    for key in keys_to_delete:
        if key in input_dict:
            del input_dict[key]

    return input_dict


def req_col(query, direct_edges):
    entities = set(query.split('/'))
    needed = set(entities)
    count = 0
    # Sort direct edges based on how many entities they satisfy
    direct_edges.sort(key=lambda x: len(set(x.split('/')) & needed), reverse=True)
    for edge in direct_edges:
        parts = edge.split('/')
        # Check if any part of the edge is needed
        if any(part in needed for part in parts):
            count += 1
            # Remove the parts that are satisfied by this edge
            needed -= set(parts)
            if not needed:
                break
    return count


def dirEdgeScore(query, direct_edges):

    # Initialize the total count of matching pairs
    total_count = 0

    q_parts = query.split('/')
    if len(q_parts) == 1:
        total_count = 0

    # For each entity in the query
    for i in range(len(q_parts)-1):
        curr = q_parts[i]
        nex = q_parts[i+1]
        for j in range(len(direct_edges)):
            dir_edge = direct_edges[j]
            if curr+"/"+nex == dir_edge:
                total_count += 1

    return round(total_count/query.count('/'), 2) if query.count('/') > 0 else 0


def allEdgeScore(query, all_edges):

    # Initialize the total count of matching pairs
    total_count = 0

    q_parts = query.split('/')
    if len(q_parts) == 1:
        total_count = 0

    # For each entity in the query
    for i in range(len(q_parts)-1):
        curr = q_parts[i]
        nex = q_parts[i+1]
        for j in range(len(all_edges)):
            dir_edge = all_edges[j]
            if curr+"/"+nex == dir_edge:
                total_count += 1

    return round(total_count/query.count('/'), 2) if query.count('/') > 0 else 0


class Path:
    def __init__(self, entities):
        self.entities = entities

    def __str__(self):
        return " -> ".join(entity for entity in self.entities)\



def pathScore(query, paths):
    
    if "?" in query:
        query = query.split("?")[0]
    q_parts = query.split('/')

    matching_paths = []

    for path in paths:
        if query in path:
            matching_paths.append(path)

    # return matching_paths if matching_paths else "q"
    if matching_paths:
        # Sort the paths based on the index of the first occurrence of the query
        sorted_paths = sorted(matching_paths, key=lambda path: path.index(query))
        nearest_path = sorted_paths[0]  # Get the path with the nearest match to the root

        # Calculate the score based on the position of the first entity of the query
        hit = nearest_path.split("/")
        for i in range(len(hit)):
            if hit[i] == q_parts[0]:
                return round(1/(i+1), 2)
            
    else:
        return 0
    

class Entity:
    def __init__(self, name):
        self.name = name
        self.children = []


def generate_combinations2(entities):
    all_combinations = []

    # Generate combinations of different lengths
    for length in range(2, len(entities) + 1):
        for combo in combinations(entities, length):
            for perm in permutations(combo):
                # Generate all possible combinations of 'embeds' and 'references'
                for relationship in product(['embeds', 'references'], repeat=length-1):
                    combinations_str = ' and '.join([f"{x} {r} {y}" for x, r, y in zip(perm[:-1], relationship, perm[1:])])
                    all_combinations.append(combinations_str)

    return all_combinations


def generate_dag2(schema):
    relationships = schema.split(";")
    dag_entities = {}
    dag_paths = []
    all_edges = []

    # Parse relationships and create entities
    for rel in relationships:
        parts = rel.split()
        if len(parts) < 3:
            # Handle incomplete relationships
            continue

        src = parts[0]
        rel_type = parts[1]
        dest = parts[2]

        if src not in dag_entities:
            dag_entities[src] = Entity(src)
        if dest not in dag_entities:
            dag_entities[dest] = Entity(dest)

        if rel_type == 'embeds':
            dag_entities[src].children.append(dag_entities[dest])
            dag_paths.append(f"{src}^{dest}")
            all_edges.append(f"{src}^{dest}")
            all_edges.append(f"{dest}^{src}")
        elif rel_type == 'references':
            dag_entities[src].children.append(dag_entities[dest])
            dag_paths.append(f"{src}*{dest}")
            all_edges.append(f"{src}*{dest}")
            all_edges.append(f"{dest}*{src}")

    return dag_entities, dag_paths, all_edges


class Path:
    def __init__(self, entities):
        self.entities = entities

    def __str__(self):
        return " -> ".join(entity for entity in self.entities)

def generate_indirect_paths2(direct_edges):
    # Dictionary to store direct connections for quick lookup
    direct_map = {}
    for edge in direct_edges:
        source, dest = edge.split('^') if '^' in edge else edge.split('*')
        if source not in direct_map:
            direct_map[source] = []
        direct_map[source].append((dest, '^' if '^' in edge else '*'))
    
    # Function to perform DFS and collect all indirect paths
    def find_all_paths(current_node, current_path, visited, all_paths):
        if current_node in visited:
            return
        
        visited.add(current_node)
        if current_node in direct_map:
            for next_node, symbol in direct_map[current_node]:
                # Extend the current path with the next node and symbol
                new_path = current_path + [symbol, next_node]
                find_all_paths(next_node, new_path, visited, all_paths)
        
        # Add the completed current path to all_paths when we reach a leaf node
        all_paths.append(''.join(current_path))
        visited.remove(current_node)
    
    all_paths = []
    # Start DFS from each node in direct_map to collect all possible paths
    for start_node in direct_map:
        find_all_paths(start_node, [start_node], set(), all_paths)
    
    # Sort all_paths by length in descending order
    all_paths.sort(key=len, reverse=True)
    
    longest_paths = []
    seen_paths = set()  # Set to keep track of paths that are already added as longest_paths
    
    # Iterate over all_paths and add longest paths to the result
    for path in all_paths:
        if not any(path.startswith(existing_path) for existing_path in seen_paths):
            longest_paths.append(path)
            seen_paths.add(path)

    
    diction = get_lengths_dictionary2(longest_paths)
    final = remove_substring_keys2(diction)
    final_paths = list(final.keys())
    
    return final_paths


def get_lengths_dictionary2(edges_list):
    lengths_dict = {}
    for edge in edges_list:
        lengths_dict[edge] = len(edge.split('*')) + len(edge.split('^')) - 1
    
    sorted_dict = dict(sorted(lengths_dict.items(), key=lambda item: item[1], reverse=True))
    return sorted_dict
def remove_substring_keys2(input_dict):

    # Create a list of keys to delete
    keys_to_delete = []

    # Iterate over each key in the dictionary
    for key in input_dict:
        # Check if any substring of the current key is also a key in the dictionary
        for other_key in input_dict:
            if key != other_key and key in other_key:
                # If a substring of 'key' exists in 'other_key', mark 'key' for deletion
                keys_to_delete.append(key)
                break  # No need to check further for this 'key'

    # Remove keys marked for deletion from the dictionary
    for key in keys_to_delete:
        if key in input_dict:
            del input_dict[key]

    return input_dict


def colExistence(direct_edges, entity_name):
    
    cols = []

    for en in entity_name:
        pattern = re.compile(re.escape(en))
        
        for edge in direct_edges:
            if pattern.match(edge):
                if en not in cols:
                    cols.append(en)
    return len(cols)


def globalDepth(direct_edges):
    max_depth = 0
    for edge in direct_edges:
        depth = edge.count("^")
        if depth > max_depth:
            max_depth = depth
    return max_depth


def docTypeCopies(direct_edges):
    cop = 0
    for edge in direct_edges:
        if '^' in edge:
            cop += edge.count('^')
    return cop


def refLoad(direct_edges):
    load = 0
    for ede in direct_edges:
        if "*" in ede:
            load += ede.count('*')
    return load


def docCopiesInCol(direct_edges):
    ex = []

    for i in direct_edges:
        # Define the regular expression pattern to capture delimiters ('*' or '^')
        pattern = r'([*^].*?(?=[*^]|$))'

        # Split the string using the pattern and filter out empty strings
        parts = [part for part in re.split(pattern, i) if part]
        for part in parts:
            if part.startswith('^'):
                ex.append(part[1:])

        # Use Counter to count occurrences of each item in the list
        counts = Counter(ex)
        # Filter items that have counts greater than 1 (repeated items)
        repeats = {item: count for item, count in counts.items() if count > 1}

        if not repeats:
            return 0  # No repeated items found
        else:
            # Sort the repeated items dictionary by counts in descending order
            sorted_repeats = dict(sorted(repeats.items(), key=lambda x: x[1], reverse=True))
            first_item = next(iter(sorted_repeats.items()), None)
            return first_item[1]





# ******************** MAIN FUNCTION ********************
if __name__ == '__main__':
    app.run(debug=True)