"""Module to define the Histogram and Axis classes"""

# columnar analysis
import hist
import awkward as ak


class Histogram:
    """Class to represent histograms

    Histogram mostly exists so that hist.Hists and the appropriate filling arguments can be
    defined in one place. In addition to the filling function associated with each Axis, the user
    can optionally provide an event mask that is applied to all object collections used to fill
    the histogram, e.g. to ensure only events with >=2 muons are used to fill dR(mu, mu) hists.
    """

    def __init__(self, axes, storage="weight", evt_mask=None):
        self.axes = axes
        self.storage = storage
        # Allow all events to pass if no mask is explicitly provided
        self.evt_mask = (lambda objs: slice(None)) if evt_mask is None else evt_mask
        self.hist = None

    def make_hist(self, channels=None):
        """Build associated hist.Hist

        Perform outside __init__ because channels aren't known until runtime.
        """

        # optionally add channels axis to hist
        if channels is not None:
            channel_axis = hist.axis.StrCategory(channels, name="channel")
            self.axes = [Axis(channel_axis, lambda objs, mask: objs["ch"])] + self.axes

        axes = [a.axis for a in self.axes]
        self.hist = hist.Hist(*axes, storage=self.storage)

    def fill(self, objs, evt_weights):
        """Fill associated hist.Hist"""
        fill_args = {a.name: a.fill_func(objs, self.evt_mask(objs)) for a in self.axes}
        # Use last axis to define weight structure to avoid channels axis
        masked_weights = evt_weights[self.evt_mask(objs)]
        fill_args["weight"] = masked_weights*ak.ones_like(fill_args[self.axes[-1].name])
        for name in fill_args.keys():
            if name != "channel":
                fill_args[name] = ak.flatten(fill_args[name], axis=None)

        try:
            self.hist.fill(**fill_args)
        except ValueError:
            print("Warning: a histogram with the following axis names could not be filled and will"
                  f" be skipped: {list(fill_args.keys())}")


class Axis:
    """Class to represent histogram axes

    Axis just bundles together hist.axis objects and functions to fill them.
    """

    def __init__(self, axis, fill_func):
        self.axis = axis
        self.name = self.axis.name
        self.fill_func = fill_func
