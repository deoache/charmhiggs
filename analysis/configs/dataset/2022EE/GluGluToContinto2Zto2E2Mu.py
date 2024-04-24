from analysis.configs.dataset_config import DatasetConfig

dataset_config = DatasetConfig(
    name="GluGluToContinto2Zto2E2Mu",
    path=(
        "root://maite.iihe.ac.be:1094//store/user/daocampo/PFNano_Run3/"
        "mc_summer22EE_MINIAODv4/GluGluToContinto2Zto2E2Mu_TuneCP5_13p6TeV_mcfm701-pythia8/"
        "Run3Summer22EEMiniAODv4-130X_mcRun3_2022_realistic_postEE_v6-v2_BTV_Run3_2022_Comm_MINIAODv4/240325_122607/0000/"
    ),
    key="Events",
    year="2022EE",
    is_mc=True,
    xsec=6.115,
    partitions=1,
    stepsize=10000,
    filenames=(
        "MC_defaultAK4_1.root",
        "MC_defaultAK4_2.root",
    )
)
