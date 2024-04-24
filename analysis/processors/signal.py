import hist
import copy
import numpy as np
import awkward as ak
from coffea import processor
from analysis.processors.utils import normalize
from coffea.analysis_tools import PackedSelection, Weights


class SignalProcessor(processor.ProcessorABC):
    def __init__(self, year):
        self.year = year

        region_axis = hist.axis.StrCategory([], name="region", growth=True)

        higgs_mass_axis = hist.axis.Regular(
            bins=50, start=10, stop=150, name="higgs_mass", label=r"$m(H)$ [GeV]"
        )
        higgs_mass_hist = hist.Hist(region_axis, higgs_mass_axis, hist.storage.Weight())

        higgs_pt_axis = hist.axis.Regular(
            bins=50, start=0, stop=300, name="higgs_pt", label=r"$p_T(H)$ [GeV]"
        )
        higgs_pt_hist = hist.Hist(region_axis, higgs_pt_axis, hist.storage.Weight())

        mass_z1_axis = hist.axis.Regular(
            bins=50, start=10, stop=150, name="z1_mass", label=r"$m(Z)$ [GeV]"
        )
        mass_z1_hist = hist.Hist(region_axis, mass_z1_axis, hist.storage.Weight())

        mass_z2_axis = hist.axis.Regular(
            bins=50, start=10, stop=150, name="z2_mass", label=r"$m(Z^*)$ [GeV]"
        )
        mass_z2_hist = hist.Hist(region_axis, mass_z2_axis, hist.storage.Weight())

        higgs_pt_axis = hist.axis.Regular(
            bins=50, start=0, stop=300, name="higgs_pt", label=r"$p_T(H)$ [GeV]"
        )
        higgs_pt_hist = hist.Hist(region_axis, higgs_pt_axis, hist.storage.Weight())

        jet_pt_axis = hist.axis.Variable(
            edges=[30, 60, 90, 120, 150, 180, 210, 240, 300, 500],
            name="jet_pt",
            label="Jet $p_T$ [GeV]",
        )
        jet_pt_hist = hist.Hist(region_axis, jet_pt_axis, hist.storage.Weight())

        jet_eta_axis = hist.axis.Regular(
            bins=50, start=-2.5, stop=2.5, name="jet_eta", label="Jet $\eta$"
        )
        jet_eta_hist = hist.Hist(region_axis, jet_eta_axis, hist.storage.Weight())

        jet_phi_axis = hist.axis.Regular(
            bins=50, start=-np.pi, stop=np.pi, name="jet_phi", label="Jet $\phi$"
        )
        jet_phi_hist = hist.Hist(region_axis, jet_phi_axis, hist.storage.Weight())

        self.hist_dict = {
            "higgs_mass": higgs_mass_hist,
            "higgs_pt": higgs_pt_hist,
            "z1_mass": mass_z1_hist,
            "z2_mass": mass_z2_hist,
            "jet_pt": jet_pt_hist,
            "jet_eta": jet_eta_hist,
            "jet_phi": jet_phi_hist,
        }

    def process(self, events):
        # get dataset name
        dataset = events.metadata["dataset"]

        # get number of events before selection
        nevents = len(events)

        # check if sample is MC
        is_mc = hasattr(events, "genWeight")

        # create copies of histogram objects
        hist_dict = copy.deepcopy(self.hist_dict)

        # dictionary to store output data and metadata
        output = {}
        output["metadata"] = {}

        # set weights container
        weights_container = Weights(len(events), storeIndividual=True)
        if is_mc:
            weights_container.add("genweight", events.genWeight)
        # save sum of weights to metadata
        output["metadata"].update({"sumw": ak.sum(weights_container.weight())})

        # -----------------------------
        # selecting a Higgs candidate
        # -----------------------------
        # impose some quality and minimum pt cuts on muons
        muons = events.Muon
        muons = muons[
            (muons.pt > 5)
            & (np.abs(muons.eta) < 2.4)
            & (muons.dxy < 0.5)
            & (muons.dz < 1)
            & (muons.pfRelIso04_all < 0.35)
            & (muons.sip3d < 4)
            & (muons.mediumId)
        ]
        # get all muon pair combinations
        dimuons = ak.combinations(muons, 2, axis=1, fields=["mu1", "mu2"])

        # get muon pairs with a deltaR separation greater than 0.02
        mu1, mu2 = dimuons["mu1"], dimuons["mu2"]
        dr_mask = mu1.delta_r(mu2) > 0.02
        dimuons = dimuons[dr_mask]
        
        # get opposite sign dimuons
        dimuons = dimuons[dimuons["mu1"].charge * dimuons["mu2"].charge < 0]

        # get dimuons with loose or tight mass windows
        z_mass = (dimuons["mu1"] + dimuons["mu2"]).mass
        loose_mass_window_mask = (
            ((z_mass > 12) & (z_mass < 120))
            & dimuons["mu1"].tightId
            & dimuons["mu2"].tightId
        )
        tight_mass_window_mask = (z_mass > 80) & (z_mass < 100)
        dimuons = dimuons[loose_mass_window_mask | tight_mass_window_mask]

        # compute |mass(mu1, mu2) - mass(Z)|
        mass_diffs = np.abs((dimuons["mu1"] + dimuons["mu2"]).mass - 91.118)

        # sort dimuons index from smallest to largest difference
        mass_diffs_idx = ak.argsort(mass_diffs, axis=1)

        # select Z candidate with minimun mass difference
        z_cands_idx = ak.singletons(ak.firsts(mass_diffs_idx))
        z_cand = dimuons[z_cands_idx]
        z_cand_p4 = z_cand["mu1"] + z_cand["mu2"]

        # the other candidate is considered to be the off-shell Z* candidate
        z_star_cands_idx = ak.singletons(ak.firsts(mass_diffs_idx[:, 1:]))
        z_star_cand = dimuons[z_star_cands_idx]
        z_star_cand_p4 = z_star_cand["mu1"] + z_star_cand["mu2"]

        # -----------------------------
        # selecting a Jet candidate
        # -----------------------------
        # impose some quality and minimum pt cuts on jets
        jets = events.Jet
        jets = jets[(jets.pt >= 30) & (np.abs(jets.eta) < 2.4) & (jets.jetId == 6)]
        jets = jets[ak.all(jets.metric_table(muons) > 0.4, axis=-1)]
        # get cjets using deepjet, particlenet and partRobust taggers
        tagger_jets = {
            "deepjet": jets[
                (jets.btagDeepFlavCvB > 0.241) & (jets.btagDeepFlavCvL > 0.305)
            ],
            "pnet": jets[(jets.btagPNetCvB > 0.258) & (jets.btagPNetCvL > 0.491)],
            "part": jets[
                (jets.btagRobustParTAK4CvB > 0.095)
                & (jets.btagRobustParTAK4CvL > 0.358)
            ],
        }
        # -----------------------------
        # event selection
        # -----------------------------
        selections = PackedSelection()
        selections.add("leadingmuonpt", ak.firsts(muons).pt > 20)
        selections.add("subleadingmuonpt", ak.firsts(muons[:, 1:]).pt > 10)
        selections.add("atleast4muons", ak.num(muons) >= 4)
        selections.add("atleast2candidates", ak.num(dimuons) >= 2)
        selections.add("onedeepjet", ak.num(tagger_jets["deepjet"]) == 1)
        selections.add("onepnetjet", ak.num(tagger_jets["pnet"]) == 1)
        selections.add("onepartjet", ak.num(tagger_jets["part"]) == 1)

        regions = {
            "deepjet": [
                "leadingmuonpt",
                "subleadingmuonpt",
                "atleast4muons",
                "atleast2candidates",
                "onedeepjet",
            ],
            "pnet": [
                "leadingmuonpt",
                "subleadingmuonpt",
                "atleast4muons",
                "atleast2candidates",
                "onepnetjet",
            ],
            "part": [
                "leadingmuonpt",
                "subleadingmuonpt",
                "atleast4muons",
                "atleast2candidates",
                "onepartjet",
            ],
        }
        # -----------------------------
        # histogram filling
        # -----------------------------
        for region in regions:
            region_selection = selections.all(*regions[region])
            # get region objects
            region_z_cand_p4 = z_cand_p4[region_selection]
            region_z_star_cand_p4 = z_star_cand_p4[region_selection]
            region_jets = tagger_jets[region][region_selection]
            # compute features
            features = {
                "higgs_mass": (region_z_cand_p4 + region_z_star_cand_p4).mass,
                "higgs_pt": (region_z_cand_p4 + region_z_star_cand_p4).pt,
                "z1_mass": region_z_cand_p4.pt,
                "z2_mass": region_z_star_cand_p4.pt,
                "jet_pt": region_jets.pt,
                "jet_eta": region_jets.eta,
                "jet_phi": region_jets.phi,
            }
            # get region weights
            region_weights = weights_container.weight()[region_selection]
            # fill histograms
            for feature, array in features.items():
                fill_args = {
                    feature: normalize(array),
                    "weight": region_weights,
                    "region": region,
                }
                hist_dict[feature].fill(**fill_args)
                
        output["histograms"] = hist_dict
        return {dataset: output}

    def postprocess(self, accumulator):
        pass