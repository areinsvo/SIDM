"""Module to define the Selection and JaggedSelection classes"""

# columnar analysis
from coffea.analysis_tools import PackedSelection
# local
from sidm.definitions.cuts import evt_cut_defs, obj_cut_defs


class Selection:
    """Class to represent the collection of cuts that define a Selection

    A selection consists of event-level cuts which reject whole events.
    Cuts are stored as a PackedSelection.

    All available cuts are defined in sidm.definitions.cuts. The specific cuts that define each
    selection are accepted by Selection() as lists of strings.
    """

    def __init__(self, cuts, verbose=False):
        self.evt_cuts = cuts # list of names of cuts to be applied
        self.all_evt_cuts = PackedSelection() # will be filled later when cuts are evaluated
        self.verbose = verbose

    def apply_evt_cuts(self, objs):
        """Evaluate all event cuts and apply results to object collections"""

        # evaluate all selected cuts
        for cut in self.evt_cuts:
            if self.verbose:
                print("Applying cut:", cut)
            try:
                self.all_evt_cuts.add(cut, evt_cut_defs[cut](objs))
            except Exception as e:
                print(f"Warning: Unable to evaluate {cut} Skipping.",e)

        # apply event cuts to object collections
        sel_objs = {}
        for name, obj in objs.items():
            try:
                sel_objs[name] = obj[self.all_evt_cuts.all(*self.evt_cuts)]
            except:
                print(f"Warning: Unable to apply event cuts to {name}. Skipping.")
        return sel_objs


class JaggedSelection:
    """Class to represent the collection of cuts that define a JaggedSelection

    A JaggedSelection consists of object-level cuts (for example, electron or lepton-jet-level cuts).
    Object-level cuts slim object collections and are stored as a dictionary of masks.

    All available cuts are defined in sidm.definitions.cuts. The specific cuts that define each
    selection are accepted by JaggedSelection() as lists of strings.
    """

    def __init__(self, cuts, verbose=False):
        self.obj_cuts = cuts # dict of cuts to be applied
        self.verbose = verbose
    
    def apply_obj_cuts(self, objs):
        """Apply object cuts sequentially"""
        sel_objs = objs.copy()
        for obj, cuts in self.obj_cuts.items():
            if obj not in objs:
                print(f"Warning: {obj} not found in sample. "
                      f"The following cuts will not be applied: {cuts}")
                continue

            for cut in cuts:
                if self.verbose:
                    print(f"Applying {obj} {cut}")
                try:
                    # Eventually, will need to use the first syntax for all cuts 
                    # (and will need to change every cut definition in cuts.py)
                    if obj  == "muons":
                        sel_objs[obj] = sel_objs[obj][ obj_cut_defs[obj][cut](sel_objs,sel_objs[obj]) ]
                    else:
                        sel_objs[obj] = sel_objs[obj][obj_cut_defs[obj][cut](sel_objs)]
                except:
                    print(f"Warning: Unable to apply {cut} for {obj}. Skipping.")
        return sel_objs

class NestedSelection:
    """Class to represent the collection of cuts that define a NestedSelection

    A NestedSelection consists of object-level cuts to the matched objects 
    (for example, the pf muons matched to dsa muons).
    
    All available cuts are defined in sidm.definitions.cuts. The specific cuts that define each
    selection are accepted by NestedSelection() as lists of strings.
    """

    def __init__(self, cuts, verbose=False):
        self.obj_cuts = cuts # dict of cuts to be applied
        self.verbose = verbose
    
    def apply_obj_cuts(self, all_objs, nested_objs, obj_name): 
        """Apply object cuts sequentially"""
        # For example, all_objs would be the full sel_objs in the processor
        # and nested_objs could be sel_objs["dsaMuons"].matched_muons in the processor
        # Then obj_name would be "muons", because we want the *muon* cuts applied to these objects
        #sel_objs = objs.copy() 
        sel_objs = nested_objs
        
        for obj, cuts in self.obj_cuts.items():
            if obj == obj_name:
                
                if self.verbose: print(f"Applying cuts to the nested {obj} collection")

                for cut in cuts:
                    try:
                        #sel_objs["dsaMuons"]=sel_objs["dsaMuons"] #This works
                        #sel_objs["dsaMuons"].matched_muons = sel_objs["dsaMuons"].matched_muons #This doesn't
                        sel_objs = sel_objs[ obj_cut_defs[obj][cut](all_objs, sel_objs)]
                    except Exception as e:
                        print(f"Warning: Unable to apply {cut} for nested {obj_name} collection. Skipping.... {e}")
                return sel_objs
 

        if self.verbose: 
            print(f"Warning: Did not find cuts for {obj_name} in the config. "
                      f"No cuts will be applied to the nested objects")
        return sel_objs
 
