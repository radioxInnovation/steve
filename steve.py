"""
title: STEVE Pipeline
author: radiox-innovation
date: 2024-10-21
version: 1.0
license: MIT
description: A pipeline for standard templates.
requirements: mako, pyyaml, ollama, openai 
"""

from typing import List, Union, Generator, Iterator, Optional
from pydantic import BaseModel
import requests
import subprocess
import sys
import os
import yaml
import re
from urllib.parse import urlparse
from mako.template import Template
from mako.runtime import Context
import base64
import zipfile
import io
import shutil
import uuid

class Pipeline:

    def __init__(self):
        self.name = "STEVE"

    def install_and_import(self, package):
        try:
            __import__(package)
            print(f"{package} is already installed.")
        except ImportError:
            print(f"{package} is not installed. Starting installation.")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])

    def spit_system_prompt( self, body, root ):
        system_template = next((msg["content"] for msg in body["messages"] if msg["role"] == "system"), "")
        header = {}

        match = re.match(r"^data:application/zip;base64,([^\n]+)$", system_template) 
        if match:
            base64_data = match.group(1)
            file_data = base64.b64decode( base64_data )
            system_template = ""

            # extract
            with zipfile.ZipFile(io.BytesIO(file_data)) as zip_file:
                zip_file.extractall(path=root)

                for file_name in zip_file.namelist():
                    if file_name.endswith("system.mako"):
                        with zip_file.open(file_name) as file:
                            system_template = file.read().decode('utf-8') 

        yaml_match = re.match( r'---\s*\n(.*?)\n---\s*\n?(.*)', system_template, re.DOTALL )
        if yaml_match:
            header = yaml.safe_load( yaml_match.group(1) )
            system_template = yaml_match.group(2) if yaml_match.group(2) else ""
        return header, system_template

    def render( self, system_template, header, root):
        try:
            header["root"] = root
            template = Template( system_template )
            buf = io.StringIO()
            ctx = Context(buf, **header )
            template.render_context(ctx)
            system_prompt = str ( buf.getvalue() )

            function_names = [ "pipe", "inlet", "outlet"]
            interface_functions = {}

            for f in function_names:
                if hasattr(ctx, f) and callable(getattr(ctx, f)):
                    interface_functions[f] = getattr(ctx, f)
            return system_prompt, interface_functions
        except Exception as e:
            return f"Error processing template: {e}. Output this error massage at the beginning of each response", {}

    def process_files( self, header, temp_dir_path ):
        files = {}
        try:
            if "files" in header:
                files = header.get("files", {})
                for filename in files.keys():
                    file_info = files[ filename ]
                    if "url" in file_info:
                        url = file_info[ 'url' ]
                        save =  file_info.get( 'save', True )
                        overwrite = file_info.get( 'overwrite', True )
                        extract = file_info.get( 'extract', True )
                        
                        try:

                            if url.startswith("data:"):
                                # data url
                                base64_data = url.split(",", 1)[1]
                                file_data = base64.b64decode(base64_data)
                            else:
                                # external url
                                response = requests.get(url)
                                response.raise_for_status()
                                file_data = response.content

                            if save:
                                full_path = os.path.abspath( os.path.join( temp_dir_path, filename) )
                                path = os.path.dirname(full_path)
                                file_info["path"] = path

                                try:
                                    os.makedirs(path, exist_ok=True)
                                except OSError as e:
                                    return self.log(f"Error creating directory {path}: {e}")

                                if filename.endswith(".zip") and extract:
                                    if os.path.exists(path) and overwrite:
                                        shutil.rmtree(path)
                                        os.makedirs(path, exist_ok=True)
                                        print(f"Existing directory deleted and recreated: {path}")
                                    
                                    if not overwrite and os.path.exists(path):
                                        return self.log(f"Skipping ZIP file {filename}, as overwrite=False and directory exists.")

                                    # extract
                                    with zipfile.ZipFile(io.BytesIO(file_data)) as zip_file:
                                        zip_file.extractall(path=path)
                                        print(f"ZIP file extracted to: {path}")
                                else:
                                    # Normale Datei schreiben
                                    if overwrite or not os.path.exists(full_path):
                                        with open(full_path, 'wb') as file:
                                            file.write(file_data)
                                        print(f"File saved: {full_path}")
                            else:
                                file_info.pop('url', None)
                                file_info["file"] = io.BytesIO(file_data)

                        except (OSError, base64.binascii.Error, zipfile.BadZipFile, requests.RequestException) as e:
                            return self.log(f"Error saving or extracting file {filename}: {e}")
        except Exception as e:
            return self.log(f"Error processing files: {e}")
        
        return files

    def log( self, msg ):
        print ( msg )

    def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:

        root = os.path.join("/app", "steve", str(uuid.uuid4()))
        os.makedirs(root, exist_ok=True)

        try:
            header, system_template = self.spit_system_prompt( body, root )
        except Exception as e:
            return self.log( f"Failed to parse yaml header: {e}" )

        try:
            if "requirements" in header:
                for package in header["requirements"]:
                    self.install_and_import( package )
        except Exception as e:
            return self.log( f"Failed to install requirements {e}" )

        files = self.process_files( header, root )

        system, interface_functions = self.render( system_template, header, root )
        
        body["messages"] = [message for message in body["messages"] if message["role"] != "system"]       
        body["messages"].insert(0, {"role": "system", "content": system } )

        inlet_func = interface_functions.get("inlet", lambda x: x)
        body = inlet_func( body )

        class CustomResponse:
            def __init__(self ):
                self.responses = []

            def process(self, text ):
                return [ text ]

            def final(self ):
                return [ ]

        try:
            response_class = interface_functions.get( "outlet", CustomResponse )

        except Exception as e:
            return self.log( f"Failed to find outlet class {e}" )

        pipe_func = interface_functions.get("pipe", None)

        custom_response = response_class ( )

        if pipe_func:
            for pipe_resp in pipe_func( user_message, model_id, messages, body ):
                for resp in custom_response.process( pipe_resp ):
                    yield resp
                for resp in custom_response.final():
                    yield resp

        elif "ollama_url" in header and "model" in header:
            OLLAMA_URL = header.get("ollama_url", "http://host.docker.internal:11434")
            MODEL = header.get("model", "llama3.2:latest")

            from ollama import Client
            client = Client( host = ollama_url )
            response = client.chat(model=ollama_model, messages=body["messages"], stream=body["stream"] )

            if body["stream"]:
                for chunk in response:
                    try:
                        line = chunk['message']['content'] or ""
                        
                        if type( line ) == str:
                            for resp in custom_response.process( line ):
                                yield resp

                        if chunk["done"] == True:
                            for resp in custom_response.final():
                                yield resp

                    except Exception as e:
                        yield f"Failed to process chunk {e}"
            else:
                for resp in custom_response.process( response['message']['content'] ):
                    yield resp

                for resp in custom_response.final():
                    yield resp

        elif "open_ai_api_key" in header and "model" in header:
            url = header.get ( "url", "https://api.openai.com/v1/chat/completions" )
            from openai import OpenAI
            client = OpenAI( api_key = header["open_ai_api_key"] )

            response = client.chat.completions.create( model=header["model"], messages= body["messages"], stream=body["stream"] )

            if body["stream"]:
                for chunk in response:
                    try:
                        line = chunk.choices[0].delta.content or ""
                        
                        if type( line ) == str:
                            for resp in custom_response.process( line ):
                                yield resp
                        if chunk.choices[0].finish_reason == 'stop':
                            for resp in custom_response.final():
                                yield resp
                    except Exception as e:
                        yield f"Failed to process chunk {e}"
            else:
                for resp in custom_response.process( response.choices[0].message.content ):
                    yield resp

                for resp in custom_response.final():
                    yield resp

        else:
            yield f"system: {system}"

        #if os.path.exists(temp_dir_path):
        #    shutil.rmtree(temp_dir_path)
