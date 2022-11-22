FROM quay.io/astronomer/astro-runtime:6.0.4

##### Docker Customizations below this line #####

## this is the default directory where pyenv will be installed, you can chose a different path as well
ENV PYENV_ROOT="/home/astro/.pyenv" 
# it is important to add the folder where the python version will be installed to PATH in order to retrieve it in the PythonVirtualEnvOperator
ENV PATH=${PYENV_ROOT}/bin:/home/astro/.pyenv/versions/snowpark_env/bin/:${PATH} 

## if you ever want to check your dependency conflicts for extra packages that you may require for your venv, this requires you to install pip-tools
# RUN pip-compile -h
# RUN pip-compile snowpark_requirements.txt

## install pyenv, install the required version
RUN curl https://pyenv.run | bash  && \
    eval "$(pyenv init -)" && \
    pyenv install 3.8.14 && \
    pyenv virtualenv 3.8.14 snowpark_env && \
    pyenv activate snowpark_env && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r snowpark_requirements.txt

    ## if you are using an external secrets manager use
    # source secrets_manager.env 
