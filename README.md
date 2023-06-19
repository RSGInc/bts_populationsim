# bts_populationsim

1. Install preferred Python environment management systems. I will demonstrate using Mamba, a faster conda alternative.

2. Clone the population sim git repository
```git clone https://github.com/ActivitySim/populationsim.git ./src/populationsim```

3. Create the mamba environment from the environment yaml recipe:
```mamba env create --file bts_populationsim.yml```

This will create an environment and install an editable version of populationsim as well as any supporting packages. Editable means that any changes in the `./src/populationsim` folder are reflected in the environment. This makes debugging easier to add break points or print lines.

