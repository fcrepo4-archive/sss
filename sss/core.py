import web, os, base64
from lxml import etree
from sss_logging import SSSLogger
from datetime import datetime

# get the global logger
sssl = SSSLogger()
ssslog = sssl.getLogger()

# create the global configuration
from config import CherryPyConfiguration
global_configuration = CherryPyConfiguration()

# FIXME: SWORDSpec has a lot of webpy stuff in it; needs to be cleaned and
# divided


class Namespaces(object):
    """
    This class encapsulates all the namespace declarations that we will need
    """
    def __init__(self):
        # AtomPub namespace and lxml format
        self.APP_NS = "http://www.w3.org/2007/app"
        self.APP = "{%s}" % self.APP_NS

        # Atom namespace and lxml format
        self.ATOM_NS = "http://www.w3.org/2005/Atom"
        self.ATOM = "{%s}" % self.ATOM_NS

        # SWORD namespace and lxml format
        self.SWORD_NS = "http://purl.org/net/sword/terms/"
        self.SWORD = "{%s}" % self.SWORD_NS

        # Dublin Core namespace and lxml format
        self.DC_NS = "http://purl.org/dc/terms/"
        self.DC = "{%s}" % self.DC_NS

        # RDF namespace and lxml format
        self.RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        self.RDF = "{%s}" % self.RDF_NS

        # ORE namespace and lxml format
        self.ORE_NS = "http://www.openarchives.org/ore/terms/"
        self.ORE = "{%s}" % self.ORE_NS

        # ORE ATOM
        self.ORE_ATOM_NS = "http://www.openarchives.org/ore/atom/"
        self.ORE_ATOM = "{%s}" % self.ORE_ATOM_NS

# REQUEST/RESPONSE CLASSES
#######################################################################
# These classes are used as the glue between the web.py web interface layer and the underlying sword server, allowing
# them to exchange messages agnostically to the interface

class Auth(object):
    def __init__(self, by=None, obo=None, target_owner_unknown=False):
        self.by = by
        self.obo = obo
        self.target_owner_unknown = target_owner_unknown

    def success(self):
        return self.by is not None and not self.target_owner_unknown

class SWORDRequest(object):
    """
    General class to represent any sword request (such as deposit or delete)
    """
    def __init__(self):
        """
        There are 4 HTTP sourced properties:
        - on_behalf_of  - On-Behalf-Of in HTTP; the user being deposited on behalf of
        - packaging     - Packaging in HTTP; the packaging format being used
        - in_progress   - In-Progress in HTTP; whether the deposit is complete or not from a client perspective
        - metadata_relevant - Metadata-Relevant; whether or not the deposit contains relevant metadata
        """

        self.on_behalf_of = None
        self.packaging = "http://purl.org/net/sword/package/Binary" # if this isn't populated externally, use the default
        self.in_progress = False
        self.metadata_relevant = True # the server MAY assume that it is True
        self.auth = None
        self.content_md5 = None
        self.slug = None

    def set_by_header(self, key, value):
        # FIXME: this is a webpy thing....
        """
        Convenience method to take a relevant HTTP header and its value and add it to this object.
        e.g. set_by_header("On-Behalf-Of", "richard")  Notice that the format of the headers used
        here is the web.py format which is all upper case, preceeding with HTTP_ with all - converted to _
        (for some unknown reason)
        """
        ssslog.debug("Setting Header %s : %s" % (key, value))
        if key == "HTTP_ON_BEHALF_OF":
            self.on_behalf_of = value
        elif key == "HTTP_PACKAGING" and value is not None:
            self.packaging = value
        elif key == "HTTP_IN_PROGRESS":
            self.in_progress = (value.strip() == "true")
        elif key == "HTTP_METADATA_RELEVANT":
            self.metadata_relevant = (value.strip() == "true")
        elif key == "HTTP_CONTENT_MD5":
            self.content_md5 = value
        elif key == "HTTP_SLUG":
            self.slug = value

class DepositRequest(SWORDRequest):
    """
    Class to represent a request to deposit some content onto the server
    """
    def __init__(self):
        """
        There are 3 content related properties:
        - content   -   the incoming content file to be deposited
        - atom      -   the incoming atom document to be deposited (may be None)
        - filename  -   the desired name of the incoming content
        """
        SWORDRequest.__init__(self)

        # content related
        self.content_type = "application/octet-stream"
        self.content = None
        self.atom = None
        self.filename = "unnamed.file"
        self.too_large = False

class DepositResponse(object):
    """
    Class to represent the response to a deposit request
    """
    def __init__(self):
        """
        Properties:
        - created   - was the resource created on the server
        - accepted  -   was the resource accepted by the server (but not yet created)
        - error_code    -   if there was an error, what HTTP status code
        - error     -   sword error document if relevant
        - receipt   -   deposit receipt if successful deposit
        - location  -   the Edit-URI which will be supplied to the client as the Location header in responses
        """
        self.created = False
        self.accepted = False
        self.error_code = None
        self.error = None
        self.receipt = None
        self.location = None

class MediaResourceResponse(object):
    """
    Class to represent the response to a request to retrieve the Media Resource
    """
    def __init__(self):
        """
        There are three properties:
        redirect    -   boolean, does the client need to be redirected to another URL for the media resource
        url         -   If redirect, then this is the URL to redirect the client to
        filepath    -   If not redirect, then this is the path to the file that the server should serve
        """
        self.redirect = False
        self.url = None
        self.filepath = None
        self.packaging = None

class DeleteRequest(SWORDRequest):
    """
    Class Representing a request to delete either the content or the container itself.
    """
    def __init__(self):
        """
        The properties of this class are as per SWORDRequest
        """
        SWORDRequest.__init__(self)

class DeleteResponse(object):
    """
    Class to represent the response to a request to delete the content or the container
    """
    def __init__(self):
        """
        There are 3 properties:
        error_code  -   if there was an error, the http code associated
        error       -   the sworderror if appropriate
        receipt     -   if successful and a request for deleting content (not container) the deposit receipt
        """
        self.error_code = None
        self.error = None
        self.receipt = None
        
# Operational SWORD Classes
#############################################################################
# Classes which carry out the grunt work of the SSS

class SWORDSpec(object):
    """
    Class which attempts to represent the specification itself.  Instead of being operational like the SWORDServer
    class, it attempts to just be able to interpret the supplied http headers and content bodies and turn them into
    the entities with which SWORD works.  The jury is out, in my mind, whether this class is a useful separation, but
    for what it's worth, here it is ...
    """
    def __init__(self):
        # FIXME: this is a webpy thing ...
        # The HTTP headers that are part of the specification (from a web.py perspective - don't be fooled, these
        # aren't the real HTTP header names - see the spec)
        self.sword_headers = [
            "HTTP_ON_BEHALF_OF", "HTTP_PACKAGING", "HTTP_IN_PROGRESS", "HTTP_METADATA_RELEVANT",
            "HTTP_CONTENT_MD5", "HTTP_SLUG", "HTTP_ACCEPT_PACKAGING"
        ]

        self.error_content_uri = "http://purl.org/net/sword/error/ErrorContent"
        self.error_checksum_mismatch_uri = "http://purl.org/net/sword/error/ErrorChecksumMismatch"
        self.error_bad_request_uri = "http://purl.org/net/sword/error/ErrorBadRequest"
        self.error_target_owner_unknown_uri = "http://purl.org/net/sword/error/TargetOwnerUnknown"
        self.error_mediation_not_allowed_uri = "http://purl.org/net/sword/error/MediationNotAllowed"
        self.error_method_not_allowed_uri = "http://purl.org/net/sword/error/MethodNotAllowed"
        self.error_max_upload_size_exceeded = "http://purl.org/net/sword/error/MaxUploadSizeExceeded"

    def validate_deposit_request(self, web, allow_multipart=True):
        dict = web.ctx.environ

        # get each of the allowed SWORD headers that can be validated and see if they do
        ip = dict.get("HTTP_IN_PROGRESS")
        if ip is not None and ip != "true" and ip != "false":
            return "In-Progress must be 'true' or 'false'"

        sm = dict.get("HTTP_METADATA_RELEVANT")
        if sm is not None and sm != "true" and sm != "false":
            return "Metadata-Relevant must be 'true' or 'false'"

        # there must be both an "atom" and "payload" input or data in web.data()
        webin = web.input()
        if len(webin) != 2 and len(webin) > 0:
            return "Multipart request does not contain exactly 2 parts"
        if len(webin) >= 2 and not webin.has_key("atom") and not webin.has_key("payload"):
            return "Multipart request must contain Content-Dispositions with names 'atom' and 'payload'"
        if len(webin) > 0 and not allow_multipart:
            return "Multipart request not permitted in this context"

        # if we get to here then we have a valid multipart or no multipart
        if len(webin) != 2: # if it is not multipart
            if web.data() is None: # and there is no content
                return "No content sent to the server"

        # validates
        return None

    def validate_delete_request(self, web):
        dict = web.ctx.environ

        # get each of the allowed SWORD headers that can be validated and see if they do
        ip = dict.get("HTTP_IN_PROGRESS")
        if ip is not None and ip != "true" and ip != "false":
            return "In-Progress must be 'true' or 'false'"

        sm = dict.get("HTTP_METADATA_RELEVANT")
        if sm is not None and sm != "true" and sm != "false":
            return "Metadata-Relevant must be 'true' or 'false'"
        
        # validates
        return None

    def get_deposit(self, web, auth=None, atom_only=False):
        # FIXME: this reads files into memory, and therefore does not scale
        # FIXME: this does not deal with the Media Part headers on a multipart deposit
        """
        Take a web.py web object and extract from it the parameters and content required for a SWORD deposit.  This
        includes determining whether this is an Atom Multipart request or not, and extracting the atom/payload where
        appropriate.  It also includes extracting the HTTP headers which are relevant to deposit, and for those not
        supplied providing their defaults in the returned DepositRequest object
        """
        d = DepositRequest()

        # now go through the headers and populate the Deposit object
        dict = web.ctx.environ

        # get the headers that have been provided.  Any headers which have not been provided have default values
        # supplied in the DepositRequest object's constructor
        ssslog.debug("Incoming HTTP headers: " + str(dict))
        empty_request = False
        for head in dict.keys():
            if head in self.sword_headers:
                d.set_by_header(head, dict[head])
            if head == "HTTP_CONTENT_DISPOSITION":
                ssslog.debug("Reading Header %s : %s" % (head, dict[head]))
                d.filename = self.extract_filename(dict[head])
                ssslog.debug("Extracted filename %s from %s" % (d.filename, dict[head]))
            if head == "CONTENT_TYPE":
                ssslog.debug("Reading Header %s : %s" % (head, dict[head]))
                ct = dict[head]
                d.content_type = ct
                if ct.startswith("application/atom+xml"):
                    atom_only = True
            if head == "CONTENT_LENGTH":
                ssslog.debug("Reading Header %s : %s" % (head, dict[head]))
                if dict[head] == "0":
                    empty_request = True
                cl = int(dict[head]) # content length as an integer
                if cl > global_configuration.max_upload_size:
                    d.too_large = True
                    return d

        # first we need to find out if this is a multipart or not
        webin = web.input()
        if len(webin) == 2:
            ssslog.info("Received multipart deposit request")
            d.atom = webin['atom']
            # read the zip file from the base64 encoded string
            d.content = base64.decodestring(webin['payload'])
        elif not empty_request:
            # if this wasn't a multipart, and isn't an empty request, then the data is in web.data().  This could be a binary deposit or
            # an atom entry deposit - reply on the passed/determined argument to determine which
            if atom_only:
                ssslog.info("Received Entry deposit request")
                d.atom = web.data()
            else:
                ssslog.info("Received Binary deposit request")
                d.content = web.data()

        # now just attach the authentication data and return
        d.auth = auth
        return d

    def extract_filename(self, cd):
        """ get the filename out of the content disposition header """
        # ok, this is a bit obtuse, but it was fun making it.  It's not hard to understand really, if you break
        # it down
        return cd[cd.find("filename=") + len("filename="):cd.find(";", cd.find("filename=")) if cd.find(";", cd.find("filename=")) > -1 else len(cd)]

    def get_delete(self, dict, auth=None):
        """
        Take a web.py web object and extract from it the parameters and content required for a SWORD delete request.
        It mainly extracts the HTTP headers which are relevant to delete, and for those not supplied provides thier
        defaults in the returned DeleteRequest object
        """
        d = DeleteRequest()

        # we just want to parse out the headers that are relevant
        for head in dict.keys():
            if head in self.sword_headers:
                d.set_by_header(head, dict[head])

        # now just attach the authentication data and return
        d.auth = auth
        return d
        
class Statement(object):
    """
    Class representing the Statement; a description of the object as it appears on the server
    """
    def __init__(self):
        """
        The statement has 4 important properties:
        - aggregation_uri   -   The URI of the aggregation in ORE terms
        - rem_uri           -   The URI of the Resource Map in ORE terms
        - original_deposits -   The list of original packages uploaded to the server (set with original_deposit())
        - in_progress       -   Is the submission in progress (boolean)
        - aggregates        -   the non-original deposit files associated with the item
        """
        self.aggregation_uri = None
        self.rem_uri = None
        self.original_deposits = []
        self.aggregates = []
        self.in_progress = False

        # URIs to use for the two supported states in SSS
        self.in_progress_uri = "http://purl.org/net/sword/state/in-progress"
        self.archived_uri = "http://purl.org/net/sword/state/archived"

        # the descriptions to associated with the two supported states in SSS
        self.states = {
            self.in_progress_uri : "The work is currently in progress, and has not passed to a reviewer",
            self.archived_uri : "The work has passed through review and is now in the archive"
        }

        # Namespace map for XML serialisation
        self.ns = Namespaces()
        self.smap = {"rdf" : self.ns.RDF_NS, "ore" : self.ns.ORE_NS, "sword" : self.ns.SWORD_NS}
        self.asmap = {"oreatom" : self.ns.ORE_ATOM_NS, "atom" : self.ns.ATOM_NS, "rdf" : self.ns.RDF_NS, "ore" : self.ns.ORE_NS, "sword" : self.ns.SWORD_NS}
        self.fmap = {"atom" : self.ns.ATOM_NS, "sword" : self.ns.SWORD_NS}

    def __str__(self):
        return str(self.aggregation_uri) + ", " + str(self.rem_uri) + ", " + str(self.original_deposits)
        
    def original_deposit(self, uri, deposit_time, packaging_format, by, obo):
        """
        Add an original deposit to the statement
        Args:
        - uri:  The URI to the original deposit
        - deposit_time:     When the deposit was originally made
        - packaging_format:     The package format of the deposit, as supplied in the Packaging header
        """
        self.original_deposits.append((uri, deposit_time, packaging_format, by, obo))

    def add_normalised_aggregations(self, aggs):
        for agg in aggs:
            if agg not in self.aggregates:
                self.aggregates.append(agg)

    def load(self, filepath):
        """
        Populate this statement object from the XML serialised statement to be found at the specified filepath
        """
        f = open(filepath, "r")
        rdf = etree.fromstring(f.read())
        
        aggs = []
        ods = []
        for desc in rdf.getchildren():
            packaging = None
            depositedOn = None
            deposit_by = None
            deposit_obo = None
            about = desc.get(self.ns.RDF + "about")
            for element in desc.getchildren():
                if element.tag == self.ns.ORE + "aggregates":
                    resource = element.get(self.ns.RDF + "resource")
                    aggs.append(resource)
                if element.tag == self.ns.ORE + "describes":
                    resource = element.get(self.ns.RDF + "resource")
                    self.aggregation_uri = resource
                    self.rem_uri = about
                if element.tag == self.ns.SWORD + "state":
                    state = element.get(self.ns.RDF + "resource")
                    self.in_progress = state == "http://purl.org/net/sword/state/in-progress"
                if element.tag == self.ns.SWORD + "packaging":
                    packaging = element.get(self.ns.RDF + "resource")
                if element.tag == self.ns.SWORD + "depositedOn":
                    deposited = element.text
                    depositedOn = datetime.strptime(deposited, "%Y-%m-%dT%H:%M:%SZ")
                if element.tag == self.ns.SWORD + "depositedBy":
                    deposit_by = element.text
                if element.tag == self.ns.SWORD + "depositedOnBehalfOf":
                    deposit_obo = element.text
            if packaging is not None:
                ods.append(about)
                self.original_deposit(about, depositedOn, packaging, deposit_by, deposit_obo)
        
        # sort out the ordinary aggregations from the original deposits
        self.aggregates = []
        for agg in aggs:
            if agg not in ods:
                self.aggregates.append(agg)

    def serialise(self):
        """
        Serialise this statement into an RDF/XML string
        """
        rdf = self.get_rdf_xml()
        return etree.tostring(rdf, pretty_print=True)

    def serialise_atom(self):
        """
        Serialise this statement to an Atom Feed document
        """
        # create the root atom feed element
        feed = etree.Element(self.ns.ATOM + "feed", nsmap=self.fmap)

        # create the sword:state term in the root of the feed
        state_uri = self.in_progress_uri if self.in_progress else self.archived_uri
        state = etree.SubElement(feed, self.ns.SWORD + "state")
        state.set("href", state_uri)
        meaning = etree.SubElement(state, self.ns.SWORD + "stateDescription")
        meaning.text = self.states[state_uri]

        # now do an entry for each original deposit
        for (uri, datestamp, format_uri, by, obo) in self.original_deposits:
            # FIXME: this is not an official atom entry yet
            entry = etree.SubElement(feed, self.ns.ATOM + "entry")

            category = etree.SubElement(entry, self.ns.ATOM + "category")
            category.set("scheme", self.ns.SWORD_NS)
            category.set("term", self.ns.SWORD_NS + "originalDeposit")
            category.set("label", "Orignal Deposit")

            # Media Resource Content URI (Cont-URI)
            content = etree.SubElement(entry, self.ns.ATOM + "content")
            content.set("type", "application/zip")
            content.set("src", uri)

            # add all the foreign markup

            format = etree.SubElement(entry, self.ns.SWORD + "packaging")
            format.text = format_uri

            deposited = etree.SubElement(entry, self.ns.SWORD + "depositedOn")
            deposited.text = datestamp.strftime("%Y-%m-%dT%H:%M:%SZ")

            deposit_by = etree.SubElement(entry, self.ns.SWORD + "depositedBy")
            deposit_by.text = by

            if obo is not None:
                deposit_obo = etree.SubElement(entry, self.ns.SWORD + "depositedOnBehalfOf")
                deposit_obo.text = obo

        # finally do an entry for all the ordinary aggregated resources
        for uri in self.aggregates:
            entry = etree.SubElement(feed, self.ns.ATOM + "entry")
            content = etree.SubElement(entry, self.ns.ATOM + "content")
            content.set("type", "application/octet-stream")
            content.set("src", uri)

        return etree.tostring(feed, pretty_print=True)

    def get_rdf_xml(self):
        """
        Get an lxml Element object back representing this statement
        """

        # we want to create an ORE resource map, and also add on the sword specific bits for the original deposits and the state

        # create the RDF root
        rdf = etree.Element(self.ns.RDF + "RDF", nsmap=self.smap)

        # in the RDF root create a Description for the REM which ore:describes the Aggregation
        description1 = etree.SubElement(rdf, self.ns.RDF + "Description")
        description1.set(self.ns.RDF + "about", self.rem_uri)
        describes = etree.SubElement(description1, self.ns.ORE + "describes")
        describes.set(self.ns.RDF + "resource", self.aggregation_uri)

        # in the RDF root create a Description for the Aggregation which is ore:isDescribedBy the REM
        description = etree.SubElement(rdf, self.ns.RDF + "Description")
        description.set(self.ns.RDF + "about", self.aggregation_uri)
        idb = etree.SubElement(description, self.ns.ORE + "isDescribedBy")
        idb.set(self.ns.RDF + "resource", self.rem_uri)

        # Create ore:aggreages for all ordinary aggregated files
        for uri in self.aggregates:
            aggregates = etree.SubElement(description, self.ns.ORE + "aggregates")
            aggregates.set(self.ns.RDF + "resource", uri)

        # Create ore:aggregates and sword:originalDeposit relations for the original deposits
        for (uri, datestamp, format, by, obo) in self.original_deposits:
            # standard ORE aggregates statement
            aggregates = etree.SubElement(description, self.ns.ORE + "aggregates")
            aggregates.set(self.ns.RDF + "resource", uri)

            # assert that this is an original package
            original = etree.SubElement(description, self.ns.SWORD + "originalDeposit")
            original.set(self.ns.RDF + "resource", uri)

        # now do the state information
        state_uri = self.in_progress_uri if self.in_progress else self.archived_uri
        state = etree.SubElement(description, self.ns.SWORD + "state")
        state.set(self.ns.RDF + "resource", state_uri)

        # Build the Description elements for the original deposits, with their sword:depositedOn and sword:packaging
        # relations
        for (uri, datestamp, format_uri, by, obo) in self.original_deposits:
            desc = etree.SubElement(rdf, self.ns.RDF + "Description")
            desc.set(self.ns.RDF + "about", uri)

            format = etree.SubElement(desc, self.ns.SWORD + "packaging")
            format.set(self.ns.RDF + "resource", format_uri)

            deposited = etree.SubElement(desc, self.ns.SWORD + "depositedOn")
            deposited.set(self.ns.RDF + "datatype", "http://www.w3.org/2001/XMLSchema#dateTime")
            deposited.text = datestamp.strftime("%Y-%m-%dT%H:%M:%SZ")

            deposit_by = etree.SubElement(desc, self.ns.SWORD + "depositedBy")
            deposit_by.set(self.ns.RDF + "datatype", "http://www.w3.org/2001/XMLSchema#string")
            deposit_by.text = by

            if obo is not None:
                deposit_obo = etree.SubElement(desc, self.ns.SWORD + "depositedOnBehalfOf")
                deposit_obo.set(self.ns.RDF + "datatype", "http://www.w3.org/2001/XMLSchema#string")
                deposit_obo.text = obo

        # finally do a description for the state
        sdesc = etree.SubElement(rdf, self.ns.RDF + "Description")
        sdesc.set(self.ns.RDF + "about", state_uri)
        meaning = etree.SubElement(sdesc, self.ns.SWORD + "stateDescription")
        meaning.text = self.states[state_uri]

        return rdf
        
