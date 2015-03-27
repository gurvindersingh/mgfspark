import numpy as np
import re

# Thanks to Anton Goloborodko, Lev Levitsky, as code below is initially from Pyteomics project.
# http://hg.theorchromo.ru/

_comments = '#;!/'

class PyteomicsError(Exception):
    """Exception raised for errors in Pyteomics library.

    Attributes
    ----------
    message : str
        Error message.
    """

    def __init__(self, msg):
        self.message = msg

    def __str__(self):
        return "Pyteomics error, message: %s" % (repr(self.message),)


class Charge(int):
    """A subclass of :py:class:`int`. Can be constructed from strings in "N+"
    or "N-" format, and the string representation of a :py:class:`Charge` is
    also in that format.
    """
    def __new__(cls, *args, **kwargs):
        try:
            return super(Charge, cls).__new__(cls, *args)
        except ValueError as e:
            if isinstance(args[0], str):
                try:
                    num, sign = re.match(r'^(\d+)(\+|-)$', args[0]).groups()
                    return super(Charge, cls).__new__(cls,
                        sign + num, *args[1:], **kwargs)
                except:
                    pass
            raise PyteomicsError(*e.args)

    def __str__(self):
        return str(abs(self)) + '+-'[self<0]


class ChargeList(list):
    """Just a list of :py:class:`Charge`s. When printed, looks like an
    enumeration of the list contents. Can also be constructed from such
    strings (e.g. "2+, 3+ and 4+").
    """
    def __init__(self, *args, **kwargs):
        if args and isinstance(args[0], str):
            self.extend(map(Charge,
                re.split(r'(?:,\s*)|(?:\s*and\s*)', args[0])))
        else:
            try:
                super(ChargeList, self).__init__(
                        sorted(set(args[0])), *args[1:], **kwargs)
            except:
                super(ChargeList, self).__init__(*args, **kwargs)
            self[:] = map(Charge, self)

    def __str__(self):
        if len(self) > 1:
            return ', '.join(map(str, self[:-1])) + ' and {}'.format(self[-1])
        elif self:
            return str(self[0])
        return super(ChargeList, self).__str__()


def parse_charge(s, list_only=False):
    if not list_only:
        try:
            return Charge(s)
        except PyteomicsError:
            pass
    return ChargeList(s)

def parseMGF(m_data):
    data = m_data.split('\n')
    reading_spectrum = False
    params = {}
    masses = []
    intensities = []
    charges = []
    for line in data:
        if not reading_spectrum:
            if line.strip() == 'BEGIN IONS':
                reading_spectrum = True
                # otherwise we are not interested; do nothing, just move along
        else:
            if not line.strip() or any(
                    line.startswith(c) for c in _comments):
                pass
            elif line.strip() == 'END IONS':
                reading_spectrum = False
                if 'pepmass' in params:
                    try:
                        pepmass = tuple(map(float, params['pepmass'].split()))
                    except ValueError:
                        raise PyteomicsError('MGF format error: cannot parse '
                                                 'PEPMASS = {}'.format(params['pepmass']))
                    else:
                        params['pepmass'] = pepmass + (None,) * (2 - len(pepmass))
                if isinstance(params.get('charge'), str):
                    params['charge'] = parse_charge(params['charge'], True)
                out = {'params': params,
                       'm/z array': np.array(masses),
                       'intensity array': np.array(intensities),
                       'charge array': np.ma.masked_equal(charges, 0)}
                return out
            else:
                l = line.split('=', 1)
                if len(l) > 1:  # spectrum-specific parameters!
                    params[l[0].lower()] = l[1].strip()
                elif len(l) == 1:  # this must be a peak list
                    l = line.split()
                    if len(l) >= 2:
                        try:
                            masses.append(float(l[0]))  # this may cause
                            intensities.append(float(l[1]))  # exceptions...
                            # charges.append(aux._parse_charge(l[2]) if len(l) > 2 else 0)
                        except ValueError:
                            raise PyteomicsError(
                                'Error when parsing %s. Line:\n%s' %
                                (data, line))

