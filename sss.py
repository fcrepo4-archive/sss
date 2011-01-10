""" SSS - Simple SWORD Server """

__version__ = "0.1"
__author__ = ["Richard Jones <richard@oneoverzero.com>"]
__license__ = "public domain"

import web, uuid, os, re, base64
from lxml import etree
from datetime import datetime
from zipfile import ZipFile

# SERVER CONFIGURATION
#############################################################################
# Use this class to specify all the bits of configuration that will be used
# in the sword server

class Configuration(object):
    def __init__(self):
        # The base url of the webservice where SSS is deployed
        self.base_url = "http://localhost:8080/"

        # The number of collections that SSS will create and give to users to deposit content into
        self.num_collections = 10

        # The directory where the deposited content should be stored
        self.store_dir = os.path.join(os.getcwd(), "store")

        # user details; the user/password pair should be used for HTTP Basic Authentication, and the obo is the user
        # to use for X-On-Behalf-Of requests.  Set authenticate=False if you want to test the server without caring
        # about authentication
        self.authenticate = True
        self.user = "sword"
        self.password = "sword"
        self.obo = "obo"

        # What media ranges should the app:accept element in the Service Document support
        self.app_accept = ["*/*"]

        # What packaging formats should the sword:acceptPackaging element in the Service Document support
        # The tuple is the URI of the format and your desired "q" value
        self.sword_accept_package = [
                ("http://purl.org/net/sword/package/METSDSpaceSIP", "1.0")
            ]

        # list of package formats that SSS can provide when retrieving the Media Resource
        self.sword_disseminate_package = [
            "http://purl.org/net/sword/package/default"
        ]

        # Supported package format disseminators; for the content type (dictionary key), the associated
        # class will be used to package the content for dissemination
        self.package_disseminators = {
                "application/zip;swordpackage=http://purl.org/net/sword/package/default" : DefaultDisseminator,
                "application/zip" : DefaultDisseminator
            }

        # Supported package format ingesters; for the X-Packaging header (dictionary key), the associated class will
        # be used to unpackage deposited content
        self.package_ingesters = {
                "http://purl.org/net/sword/package/default" : DefaultIngester,
                "http://purl.org/net/sword/types/METSDSpaceSIP" : METSDSpaceIngester
            }

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
        self.SWORD_NS = "http://purl.org/net/sword/"
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

# SWORD URLS
#############################################################################
# Define our URL mappings for the web service.  We are using URL parts immediately after the base of the service
# which reflect the short-hand terms used in the SWORD documentation (sd-uri, col-uri, cont-uri, em-uri and edit-uri
#
urls = (
    '/sd-uri', 'ServiceDocument',               # From which to retrieve the service document
    '/col-uri/(.+)', 'Collection',              # Representing a Collection as listed in the service document
    '/cont-uri/(.+)', 'MediaResourceContent',   # The URI used in atom:content@src
    '/em-uri/(.+)', 'MediaResource',            # The URI used in atom:link@rel=edit-media
    '/edit-uri/(.+)', 'Container',              # The URI used in atom:link@rel=edit

    # NOT PART OF SWORD: sword says nothing about how components of the item are identified, but here we use the
    # PART-URI prefix to denote parts of the object in the server
    '/part-uri/(.+)', 'Part',

    # NOT PART OF SWORD: for convenience to supply HTML pages of deposited content
    '/html/(.+)', 'WebUI'
)

# HTTP HANDLERS
#############################################################################
# Define a set of handlers for the various URLs defined above to be used by web.py

class SwordHttpHandler(object):
    def authenticate(self, web):
        auth = web.ctx.env.get('HTTP_AUTHORIZATION')
        obo = web.ctx.env.get("HTTP_X_ON_BEHALF_OF")

        cfg = Configuration()

        # we may have turned authentication off for development purposes
        if not cfg.authenticate:
            return True

        # if we want to authenticate, but there is no auth string then bouce with a 401
        if auth is None:
            web.header('WWW-Authenticate','Basic realm="SSS"')
            web.ctx.status = '401 Unauthorized'
            return False
        else:
            # assuming Basic authentication, get the username and password
            auth = re.sub('^Basic ','',auth)
            username, password = base64.decodestring(auth).split(':')

            # if the username and password don't match, bounce the user with a 401
            # meanwhile if the obo header has been passed but doesn't match the config value also bounce
            # witha 401 (I know this is an odd looking if/else but it's for clarity of what's going on
            if username != cfg.user or password != cfg.password:
                web.ctx.status = '401 Unauthorized'
                return False
            elif obo is not None and obo != cfg.obo:
                web.ctx.status = '401 Unauthorized'
                return False

        return True

class ServiceDocument(SwordHttpHandler):
    """
    Handle all requests for Service documents (requests to SD-URI)
    """
    def GET(self):
        """ GET the service document - returns an XML document """

        # authenticate
        auth = self.authenticate(web)
        if not auth:
            return

        # if we get here authentication was successful and we carry on
        ss = SWORDServer()
        web.header("Content-Type", "text/xml")
        return ss.service_document()

class Collection(SwordHttpHandler):
    """
    Handle all requests to SWORD/ATOM Collections (these are the collections listed in the Service Document) - Col-URI
    """
    def GET(self, collection):
        """
        GET a representation of the collection in XML
        Args:
        - collection:   The ID of the collection as specified in the requested URL
        Returns an XML document with some metadata about the collection and the contents of that collection
        """
        # authenticate
        auth = self.authenticate(web)
        if not auth:
            return

        # if we get here authentication was successful and we carry on
        ss = SWORDServer()
        web.header("Content-Type", "text/xml")
        return ss.list_collection(collection)

    def POST(self, collection):
        """
        POST either an Atom Multipart request, or a simple package into the specified collection
        Args:
        - collection:   The ID of the collection as specified in the requested URL
        Returns a Deposit Receipt
        """
        # authenticate
        auth = self.authenticate(web)
        if not auth:
            return

        # if we get here authentication was successful and we carry on
        ss = SWORDServer()
        spec = SWORDSpec()

        # take the HTTP request and extract a Deposit object from it
        deposit = spec.get_deposit(web)
        result = ss.deposit_new(collection, deposit)

        if result is None:
            return web.notfound()
        
        # unless this is an error, the content type is an atom entry
        web.header("Content-Type", "application/atom+xml;type=entry")
        if result.created:
            web.header("Location", result.location)
            web.ctx.status = "201 Created"
            return result.receipt
        elif result.accepted:
            web.header("Location", result.location)
            web.ctx.status = "202 Accepted"
            return result.receipt
        else:
            # if it turns out to be an error, re-write the content-type header
            # FIXME: is this how it works, or do we end up with two Content-Type headers?
            web.header("Content-Type", "text/xml")
            web.ctx.status = result.error_code
            return result.error

class MediaResourceContent(SwordHttpHandler):
    """
    Class to represent the content of the media resource.  This is the object which appears under atom:content@src, not
    the EM-URI.  It has its own class handler because it is a distinct resource, which does not necessarily resolve to
    the same location as the EM-URI.  See the Atom and SWORD specs for more details.
    """
    def GET(self, id):
        """
        GET the media resource content in the requested format (web request will include content negotiation via
        Accept header)
        Args:
        - id:   the ID of the object in the store
        Returns the content in the requested format
        """
        # NOTE: this method is not authenticated - we imagine sharing this URL with end-users who will just want
        # to retrieve the content.  It's only for the purposes of example, anyway
        ss = SWORDServer()

        # first thing we need to do is check that there is an object to return, because otherwise we may throw a
        # 415 Unsupported Media Type without looking first to see if there is even any media to content negotiate for
        # which would be weird from a client perspective
        if not ss.exists(id):
            return web.notfound()

        # do some content negotiation
        cn = ContentNegotiator()

        # if no Accept header, then we will get this back
        cn.default_type = "text/html"

        # The list of acceptable formats (in order of preference).  The tuples list the type and
        # the parameters section respectively
        cn.acceptable = [
                ContentType("application", "zip", "swordpackage=http://purl.org/net/sword/package/default"),
                ContentType("application", "zip"),
                ContentType("text", "html")
            ]

        # do the negotiation
        content_type = cn.negotiate(web.ctx.environ)

        # did we successfully negotiate a content type?
        if content_type is None:
            web.ctx.status = "415 Unsupported Media Type"
            return
        
        # if we did, we can get hold of the media resource
        media_resource = ss.get_media_resource(id, content_type)

        # either send the client a redirect, or stream the content out
        if media_resource.redirect:
            return web.found(media_resource.url)
        else:
            web.header("Content-Type", content_type.mimetype())
            f = open(media_resource.filepath, "r")
            web.ctx.status = "200 OK"
            return f.read()

class MediaResource(MediaResourceContent):
    """
    Class to represent the media resource itself (EM-URI).  This extends from the MediaResourceContent class to take advantage
    of the GET method available there.  In a real implementation of AtomPub/SWORD the MediaResource and the
    MediaResourceContent are allowed to be separate entities, which can behave differently (see the specs for more
    details).  For the purposes of SSS, we are treating them the same for convenience.
    """
    def PUT(self, id):
        """
        PUT a new package onto the object identified by the supplied id
        Args:
        - id:   the ID of the media resource as specified in the URL
        Returns a Deposit Receipt
        """
        # authenticate
        auth = self.authenticate(web)
        if not auth:
            return

        # if we get here authentication was successful and we carry on
        ss = SWORDServer()
        spec = SWORDSpec()

        # get a deposit object.  The PUT operation only supports a single binary deposit, not an Atom Multipart one
        # so if the deposit object has an atom part we should return an error
        deposit = spec.get_deposit(web)

        # FIXME: does this need me to return a SWORD Error document?  We need to look at the 1.0 SWORD spec and find
        # out what it says about Bad Requests
        if deposit.atom is not None:
            return web.badrequest()

        # next, before processing the request, let's check that the id is valid, and if not 404 the client
        if not ss.exists(id):
            return web.notfound()

        # now replace the content of the container
        result = ss.replace(id, deposit)

        # unless this is an error, the content type is an atom entry
        web.header("Content-Type", "application/atom+xml;type=entry")
        if result.created:
            web.ctx.status = "201 Created"
            return result.receipt
        elif result.accepted:
            web.ctx.status = "202 Accepted"
            return result.receipt
        else:
            # if it turns out to be an error, re-write the content-type header
            # FIXME: is this how it works, or do we end up with two Content-Type headers?
            web.header("Content-Type", "text/xml")
            web.ctx.status = result.error_code
            return result.error

    def DELETE(self, id):
        """
        DELETE the contents of an object in the store (but not the object's container), leaving behind an empty
        container for further use
        Args:
        - id:   the ID of the object to have its content removed as per the requested URI
        Return a Deposit Receipt
        """
        # authenticate
        auth = self.authenticate(web)
        if not auth:
            return

        # if we get here authentication was successful and we carry on
        ss = SWORDServer()
        spec = SWORDSpec()

        # parse the delete request out of the HTTP request
        delete = spec.get_delete(web.ctx.environ)

        # next, before processing the request, let's check that the id is valid, and if not 404 the client
        if not ss.exists(id):
            return web.notfound()

        # carry out the delete
        result = ss.delete_content(id, delete)

        # if there was an error, report it, otherwise return the deposit receipt
        if result.error_code is not None:
            web.header("Content-Type", "text/xml")
            web.ctx.status = result.error_code
            return result.error
        else:
            web.header("Content-Type", "application/atom+xml;type=entry")
            return result.receipt

class Container(SwordHttpHandler):
    """
    Class to deal with requests to the container, which is represented by the main Atom Entry document returned in
    the deposit receipt (Edit-URI).
    """
    def GET(self, id):
        """
        GET a representation of the container in the appropriate (content negotiated) format as identified by
        the supplied id
        Args:
        - id:   The ID of the container as supplied in the request URL
        Returns a representation of the container: SSS will return either the Atom Entry identical to the one supplied
        as a deposit receipt or the pure RDF/XML Statement depending on the Accept header
        """
        # authenticate
        auth = self.authenticate(web)
        if not auth:
            return

        # if we get here authentication was successful and we carry on
        ss = SWORDServer()
        spec = SWORDSpec()

        # first thing we need to do is check that there is an object to return, because otherwise we may throw a
        # 415 Unsupported Media Type without looking first to see if there is even any media to content negotiate for
        # which would be weird from a client perspective
        if not ss.exists(id):
            return web.notfound()

        # do some content negotiation
        cn = ContentNegotiator()

        # if no Accept header, then we will get this back
        cn.default_type = "application/atom+xml"
        cn.default_params = "type=entry"

        # The list of acceptable formats (in order of preference).  The tuples list the type and
        # the parameters section respectively
        cn.acceptable = [
                ContentType("application", "atom+xml", "type=entry"),
                ContentType("application", "rdf+xml")
            ]

        # do the negotiation
        content_type = cn.negotiate(web.ctx.environ)

        # did we successfully negotiate a content type?
        if content_type is None:
            web.ctx.status = "415 Unsupported Media Type"
            return

        # now actually get hold of the representation of the container and send it to the client
        cont = ss.get_container(id, content_type)
        return cont

    def POST(self, id):
        """
        POST some new content into the container identified by the supplied id
        Args:
        - id:    The ID of the container as contained in the URL
        Returns a Deposit Receipt
        """
        # authenticate
        auth = self.authenticate(web)
        if not auth:
            return

        # if we get here authentication was successful and we carry on
        ss = SWORDServer()
        spec = SWORDSpec()

        # take the HTTP request and extract a Deposit object from it
        deposit = spec.get_deposit(web)
        result = ss.deposit_existing(id, deposit)

        if result is None:
            # we couldn't find the id
            return web.notfound()
            
        # unless this is an error, the content type is an atom entry
        web.header("Content-Type", "application/atom+xml;type=entry")
        if result.created:
            web.ctx.status = "201 Created"
            return result.receipt
        elif result.accepted:
            web.ctx.status = "202 Accepted"
            return result.receipt
        else:
            # if it turns out to be an error, re-write the content-type header
            # FIXME: is this how it works, or do we end up with two Content-Type headers?
            web.header("Content-Type", "text/xml")
            web.ctx.status = result.error_code
            return result.error

    def DELETE(self, id):
        """
        DELETE the container (and everything in it) from the store, as identified by the supplied id
        Args:
        - id:   the ID of the container
        Returns nothing, as there is nothing to return (204 No Content)
        """
        # authenticate
        auth = self.authenticate(web)
        if not auth:
            return

        # if we get here authentication was successful and we carry on
        ss = SWORDServer()
        spec = SWORDSpec()
        delete = spec.get_delete(web.ctx.environ)

        # next, before processing the request, let's check that the id is valid, and if not 404 the client
        if not ss.exists(id):
            return web.notfound()

        # carry out the delete
        result = ss.delete_container(id, delete)

        # if there was an error, report it, otherwise return the deposit receipt
        if result.error_code is not None:
            web.header("Content-Type", "text/xml")
            web.ctx.status = result.error_code
            return result.error
        else:
            web.header("Content-Type", "application/atom+xml;type=entry")
            web.ctx.status = "204 No Content"
            return

class WebUI(SwordHttpHandler):
    """
    Class to provide a basic web interface to the store for convenience
    """
    def GET(self, id):
        # FIXME: this is useful but not hugely important; get to it later
        pass

class Part(SwordHttpHandler):
    """
    Class to provide access to the component parts of the object on the server
    """
    def GET(self, id):
        # FIXME: this is useful but not hugely important; get to it later
        pass

# CONTENT NEGOTIATION
#######################################################################
# A sort of generic tool for carrying out content negotiation tasks with the web interface

class ContentType(object):
    """
    Class to represent a content type requested through content negotiation
    """
    def __init__(self, type=None, subtype=None, params=None):
        """
        Properties:
        type    - the main type of the content.  e.g. in text/html, the type is "text"
        subtype - the subtype of the content.  e.g. in text/html the subtype is "html"
        params  - as per the mime specification, his represents the parameter extension to the type, e.g. with
                    application/atom+xml;type=entry, the params are "type=entry"

        So, for example:
        application/atom+xml;type=entry => type="application", subtype="atom+xml", params="type=entry"
        """
        self.type = type
        self.subtype = subtype
        self.params = params

    def mimetype(self):
        """
        Turn the content type into its mimetype representation
        """
        mt = self.type + "/" + self.subtype
        if self.params is not None:
            mt += ";" + self.params
        return mt

    def matches(self, other):
        """
        Determine whether this ContentType and the supplied other ContentType are matches.  This includes full equality
        or whether the wildcards (*) which can be supplied for type or subtype properties are in place in either
        partner in the match.
        """
        tmatch = self.type == "*" or other.type == "*" or self.type == other.type
        smatch = self.subtype == "*" or other.subtype == "*" or self.subtype == other.subtype
        # FIXME: there is some ambiguity in mime as to whether the omission of the params part is the same as
        # a wildcard.  For the purposes of convenience we have assumed here that it is, otherwise a request for
        # */* will not match any content type which has parameters
        pmatch = self.params is None or other.params is None or self.params == other.params
        return tmatch and smatch and pmatch

    def __eq__(self, other):
        return self.mimetype() == other.mimetype()

    def __str__(self):
        return str(self.type) + "/" + str(self.subtype) + ";" + str(self.params)

    def __repr__(self):
        return str(self)

class ContentNegotiator(object):
    """
    Class to manage content negotiation.  Given its input parameters it will provide a ContentType object which
    the server can use to locate its resources
    """
    def __init__(self):
        """
        There are 4 parameters which must be set in order to start content negotiation
        - acceptable    -   What ContentType objects are acceptable to return (in order of preference)
        - default_type  -   If no Accept header is found use this type
        - default_subtype   -   If no Accept header is found use this subtype
        - default_params    -   If no Accept header is found use this subtype
        """
        self.acceptable = []
        self.default_type = None
        self.default_subtype = None
        self.default_params = None

    def get_accept(self, dict):
        """
        Get the Accept header out of the web.py HTTP dictionary.  Return None if no accept header exists
        """
        if not dict.has_key("HTTP_ACCEPT"):
            return None

        return dict["HTTP_ACCEPT"]

    def analyse_accept(self, accept):
        # FIXME: we need to somehow handle q=0.0 in here and in other related methods
        """
        Analyse the Accept header string from the HTTP headers and return a structured dictionary with each
        content types grouped by their common q values, thus:

        dict = {
            1.0 : [<ContentType>, <ContentType>],
            0.8 : [<ContentType],
            0.5 : [<ContentType>, <ContentType>]
        }

        This method will guarantee that ever content type has some q value associated with it, even if this was not
        supplied in the original Accept header; it will be inferred based on the rules of content negotiation
        """
        # accept headers are a list of content types and q values, in a comma separated list
        parts = accept.split(",")

        # set up some registries for the coming analysis.  unsorted will hold each part of the accept header following
        # its analysis, but without respect to its position in the preferences list.  highest_q and counter will be
        # recorded during this first run so that we can use them to sort the list later
        unsorted = []
        highest_q = 0
        counter = 0

        # go through each possible content type and analyse it along with its q value
        for part in parts:
            # count the part number that we are working on, starting from 1
            counter += 1

            # the components of the part can be "type;params;q" "type;params", "type;q" or just "type"
            components = part.split(";")

            # the first part is always the type (see above comment)
            type = components[0].strip()

            # create some default values for the other parts.  If there is no params, we will use None, if there is
            # no q we will use a negative number multiplied by the position in the list of this part.  This allows us
            # to later see the order in which the parts with no q value were listed, which is important
            params = None
            q = -1 * counter

            # There are then 3 possibilities remaining to check for: "type;q", "type;params" and "type;params;q"
            # ("type" is already handled by the default cases set up above)
            if len(components) == 2:
                # "type;q" or "type;params"
                if components[1].strip().startswith("q="):
                    # "type;q"
                    q = components[1].strip()[2:] # strip the "q=" from the start of the q value
                    # if the q value is the highest one we've seen so far, record it
                    if int(q) > highest_q:
                        highest_q = q
                else:
                    # "type;params"
                    params = components[1].strip()
            elif len(components) == 3:
                # "type;params;q"
                params = components[1].strip()
                q = components[1].strip()[2:] # strip the "q=" from the start of the q value
                # if the q value is the highest one we've seen so far, record it
                if int(q) > highest_q:
                    highest_q = q

            # at the end of the analysis we have all of the components with or without their default values, so we
            # just record the analysed version for the time being as a tuple in the unsorted array
            unsorted.append((type, params, q))

        # once we've finished the analysis we'll know what the highest explicitly requested q will be.  This may leave
        # us with a gap between 1.0 and the highest requested q, into which we will want to put the content types which
        # did not have explicitly assigned q values.  Here we calculate the size of that gap, so that we can use it
        # later on in positioning those elements.  Note that the gap may be 0.0.
        q_range = 1.0 - highest_q

        # set up a dictionary to hold our sorted results.  The dictionary will be keyed with the q value, and the
        # value of each key will be an array of ContentType objects (in no particular order)
        sorted = {}

        # go through the unsorted list
        for (type, params, q) in unsorted:
            # break the type into super and sub types for the ContentType constructor
            supertype, subtype = type.split("/", 1)
            if q > 0:
                # if the q value is greater than 0 it was explicitly assigned in the Accept header and we can just place
                # it into the sorted dictionary
                self.insert(sorted, q, ContentType(supertype, subtype, params))
            else:
                # otherwise, we have to calculate the q value using the following equation which creates a q value "qv"
                # within "q_range" of 1.0 [the first part of the eqn] based on the fraction of the way through the total
                # accept header list scaled by the q_range [the second part of the eqn]
                qv = (1.0 - q_range) + (((-1 * q)/counter) * q_range)
                self.insert(sorted, qv, ContentType(supertype, subtype, params))

        # now we have a dictionary keyed by q value which we can return
        return sorted

    def insert(self, d, q, v):
        """
        Utility method: if dict d contains key q, then append value v to the array which is identified by that key
        otherwise create a new key with the value of an array with a single value v
        """
        if d.has_key(q):
            d[q].append(v)
        else:
            d[q] = [v]

    def contains_match(self, source, target):
        """
        Does the target list of ContentType objects contain a match for the supplied source
        Args:
        - source:   A ContentType object which we want to see if it matches anything in the target
        - target:   A list of ContentType objects to try to match the source against
        Returns the matching ContentTYpe from the target list, or None if no such match
        """
        for ct in target:
            if source.matches(ct):
                # matches are symmetrical, so source.matches(ct) == ct.matches(source) so way round is irrelevant
                # we return the target's content type, as this is considered the definitive list of allowed
                # content types, while the source may contain wildcards
                return ct
        return None

    def get_acceptable(self, client, server):
        """
        Take the client content negotiation requirements - as returned by analyse_accept() - and the server's
        array of supported types (in order of preference) and determine the most acceptable format to return.

        This method always returns the client's most preferred format if the server supports it, irrespective of the
        server's preference.  If the client has no discernable preference between two formats (i.e. they have the same
        q value) then the server's preference is taken into account.

        Returns a ContentType object represening the mutually acceptable content type, or None if no agreement could
        be reached.
        """

        # get the client requirement keys sorted with the highest q first (the server is a list which should be
        # in order of preference already)
        ckeys = client.keys()
        ckeys.sort(reverse=True)

        # the rule for determining what to return is that "the client's preference always wins", so we look for the
        # highest q ranked item that the server is capable of returning.  We only take into account the server's
        # preference when the client has two equally weighted preferences - in that case we take the server's
        # preferred content type
        for q in ckeys:
            # for each q in order starting at the highest
            possibilities = client[q]
            allowable = []
            for p in possibilities:
                # for each content type with the same q value

                # find out if the possibility p matches anything in the server.  This uses the ContentType's
                # matches() method which will take into account wildcards, so content types like */* will match
                # appropriately.  We get back from this the concrete ContentType as specified by the server
                # if there is a match, so we know the result contains no unintentional wildcards
                match = self.contains_match(p, server)
                if match is not None:
                    # if there is a match, register it
                    allowable.append(match)

            # we now know if there are 0, 1 or many allowable content types at this q value
            if len(allowable) == 0:
                # we didn't find anything, so keep looking at the next q value
                continue
            elif len(allowable) == 1:
                # we found exactly one match, so this is our content type to use
                return allowable[0]
            else:
                # we found multiple supported content types at this q value, so now we need to choose the server's
                # preference
                for i in range(len(server)):
                    # iterate through the server explicitly by numerical position
                    if server[i] in allowable:
                        # when we find our first content type in the allowable list, it is the highest ranked server content
                        # type that is allowable, so this is our type
                        return server[i]

        # we've got to here without returning anything, which means that the client and server can't come to
        # an agreement on what content type they want and can deliver.  There's nothing more we can do!
        return None

    def negotiate(self, dict):
        """
        Main method for carrying out content negotiation over the supplied HTTP headers dictionary.
        Returns either the preferred ContentType as per the settings of the object, or None if no agreement could be
        reached
        """
        # get the accept header if available
        accept = self.get_accept(dict)
        print "Accept Header: " + str(accept)
        if accept is None:
            # if it is not available just return the defaults
            return ContentType(self.default_type, self.default_subtype, self.default_params)

        # get us back a dictionary keyed by q value which tells us the order of preference that the client has
        # requested
        analysed = self.analyse_accept(accept)
        print "Analysed Accept: " + str(analysed)

        # go through the analysed formats and cross reference them with the acceptable formats
        content_type = self.get_acceptable(analysed, self.acceptable)
        print "Accepted: " + str(content_type)

        # return the acceptable content type.  If this is None (which get_acceptable can return), then the caller
        # will know that we failed to negotiate a type and should 415 the client
        return content_type

# REQUEST/RESPONSE CLASSES
#######################################################################
# These classes are used as the glue between the web.py web interface layer and the underlying sword server, allowing
# them to exchange messages agnostically to the interface


class SWORDRequest(object):
    """
    General class to represent any sword request (such as deposit or delete)
    """
    def __init__(self):
        """
        There are 4 HTTP sourced properties:
        - on_behalf_of  - X-On-Behalf-Of in HTTP; the user being deposited on behalf of
        - packaging     - X-Packaging in HTTP; the packaging format being used
        - in_progress   - X-In-Progress in HTTP; whether the deposit is complete or not from a client perspective
        - suppress_metadata - X-Suppress-Metadata; whether or not to extract metadata from the deposit
        """

        self.on_behalf_of = None
        self.packaging = "http://purl.org/net/sword/package/default"
        self.in_progress = False
        self.suppress_metadata = False

    def set_by_header(self, key, value):
        """
        Convenience method to take a relevant HTTP header and its value and add it to this object.
        e.g. set_by_header("X-On-Behalf-Of", "richard")  Notice that the format of the headers used
        here is the web.py format which is all upper case, preceeding with HTTP_ with all - converted to _
        (for some unknown reason)
        """
        if key == "HTTP_X_ON_BEHALF_OF":
            self.on_behalf_of = value
        elif key == "HTTP_X_PACKAGING":
            self.packaging = value
        elif key == "HTTP_X_IN_PROGRESS":
            self.in_progress = (value.strip() == "true")
        elif key == "HTTP_X_SUPPRESS_METADATA":
            self.suppress_metadata = (value.strip() == "true")

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
        self.content = None
        self.atom = None
        self.filename = "example.zip"

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
        # The HTTP headers that are part of the specification (from a web.py perspective - don't be fooled, these
        # aren't the real HTTP header names - see the spec)
        self.sword_headers = [
            "HTTP_X_ON_BEHALF_OF", "HTTP_X_PACKAGING", "HTTP_X_IN_PROGRESS", "HTTP_X_SUPPRESS_METADATA"
        ]

    def get_deposit(self, web):
        """
        Take a web.py web object and extract from it the parameters and content required for a SWORD deposit.  This
        includes determining whether this is an Atom Multipart request or not, and extracting the atom/payload where
        appropriate.  It also includes extracting the HTTP headers which are relevant to deposit, and for those not
        supplied providing their defaults in the returned DepositRequest object
        """
        d = DepositRequest()

        # first we need to find out if this is a multipart or not
        webin = web.input()
        if len(webin) == 2:
            d.atom = webin['atom']
            # FIXME: we know that due to the way that the multipart works, this is a base64 encoded string, which
            # does not equal a ZIP file.  Have to come back to this and figure out what is best to do
            d.content = webin['payload']
        else:
            # if this wasn't a multipart, then the data is in web.data()
            d.content = web.data()

        # now go through the headers and populate the Deposit object
        dict = web.ctx.environ

        # get the headers that have been provided.  Any headers which have not been provided have default values
        # supplied in the DepositRequest object's constructor
        for head in dict.keys():
            if head in self.sword_headers:
                d.set_by_header(head, dict[head])
            if head == "HTTP_CONTENT_DISPOSITION":
                d.filename = self.extract_filename(dict[head])

        return d

    def extract_filename(self, cd):
        """ get the filename out of the content disposition header """
        # ok, this is a bit obtuse, but it was fun making it.  It's not hard to understand really, if you break
        # it down
        return cd[cd.find("filename=") + len("filename="):cd.find(";", cd.find("filename=")) if cd.find(";", cd.find("filename=")) > -1 else len(cd)]

    def get_delete(self, dict):
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

        return d

class SWORDServer(object):
    """
    The main SWORD Server class.  This class deals with all the CRUD requests as provided by the web.py HTTP
    handlers
    """
    def __init__(self):

        # get the configuration
        self.configuration = Configuration()

        # create a DAO for us to use
        self.dao = DAO()

        # create a Namespace object for us to use
        self.ns = Namespaces()

        # create a URIManager for us to use
        self.um = URIManager()

        # build the namespace maps that we will use during serialisation
        self.sdmap = {None : self.ns.APP_NS, "sword" : self.ns.SWORD_NS, "atom" : self.ns.ATOM_NS, "dcterms" : self.ns.DC_NS}
        self.cmap = {None: self.ns.ATOM_NS}
        self.drmap = {None: self.ns.ATOM_NS, "sword" : self.ns.SWORD_NS}
        self.smap = {"rdf" : self.ns.RDF_NS, "ore" : self.ns.ORE_NS, "sword" : self.ns.SWORD_NS}

    def exists(self, oid):
        """
        Does the specified object id exist?
        """
        collection, id = oid.split("/", 1)
        return self.dao.collection_exists(collection) and self.dao.container_exists(collection, id)

    def service_document(self):
        """
        Construct the Service Document.  This takes the set of collections that are in the store, and places them in
        an Atom Service document as the individual entries
        """
        # Start by creating the root of the service document, supplying to it the namespace map in this first instance
        service = etree.Element(self.ns.APP + "service", nsmap=self.sdmap)

        # version element
        version = etree.SubElement(service, self.ns.SWORD + "version")
        version.text = "2.0" # SWORD 2.0!  Oh yes!

        # workspace element
        workspace = etree.SubElement(service, self.ns.APP + "workspace")

        # title element
        title = etree.SubElement(workspace, self.ns.ATOM + "title")
        title.text = "Main Site"

        # now for each collection create a collection element
        for col in self.dao.get_collection_names():
            collection = etree.SubElement(workspace, self.ns.APP + "collection")
            collection.set("href", self.um.col_uri(col))

            # collection title
            ctitle = etree.SubElement(collection, self.ns.ATOM + "title")
            ctitle.text = "Collection " + col

            # accepts declaration
            for acc in self.configuration.app_accept:
                accepts = etree.SubElement(collection, self.ns.APP + "accept")
                accepts.text = acc

            # SWORD collection policy
            collectionPolicy = etree.SubElement(collection, self.ns.SWORD + "collectionPolicy")
            collectionPolicy.text = "Collection Policy"

            # Collection abstract
            abstract = etree.SubElement(collection, self.ns.DC + "abstract")
            abstract.text = "Collection Description"

            # support for mediation
            mediation = etree.SubElement(collection, self.ns.SWORD + "mediation")
            mediation.text = "true"

            # treatment
            treatment = etree.SubElement(collection, self.ns.SWORD + "treatment")
            treatment.text = "Treatment description"

            # SWORD packaging formats accepted
            for format, q in self.configuration.sword_accept_package:
                acceptPackaging = etree.SubElement(collection, self.ns.SWORD + "acceptPackaging")
                acceptPackaging.text = format
                acceptPackaging.set("q", q)

        # pretty print and return
        return etree.tostring(service, pretty_print=True)

    def list_collection(self, id):
        """
        List the contents of a collection identified by the supplied id
        """
        # create an empty feed element for the collection
        feed = etree.Element(self.ns.ATOM + "feed", nsmap=self.cmap)

        # if the collection path does not exist, then return the empty feed
        cpath = os.path.join(self.configuration.store_dir, str(id))
        if not os.path.exists(cpath):
            return etree.tostring(feed, pretty_print=True)

        # list all of the containers in the collection
        parts = os.listdir(cpath)
        for part in parts:
            entry = etree.SubElement(feed, self.ns.ATOM + "entry")
            link = etree.SubElement(entry, self.ns.ATOM + "link")
            link.set("rel", "edit")
            link.set("href", self.um.edit_uri(id, part))

        # pretty print and return
        return etree.tostring(feed, pretty_print=True)

    def deposit_new(self, collection, deposit):
        """
        Take the supplied deposit and treat it as a new container with content to be created in the specified collection
        Args:
        -collection:    the ID of the collection to be deposited into
        -deposit:       the DepositRequest object to be processed
        Returns a DepositResponse object which will contain the Deposit Receipt or a SWORD Error
        """
        # does the collection directory exist?  If not, we can't do a deposit
        if not self.dao.collection_exists(collection):
            return None

        # create us a new container
        id = self.dao.create_container(collection)

        # store the incoming atom document if necessary
        if deposit.atom is not None:
            self.dao.store_atom(collection, id, deposit.atom)

        # store the content file
        fn = self.dao.store_content(collection, id, deposit.content, deposit.filename)

        # now that we have stored the atom and the content, we can invoke a package ingester over the top to extract
        # all the metadata and any files we want
        packager = self.configuration.package_ingesters[deposit.packaging]()
        packager.ingest(collection, id, fn, deposit.suppress_metadata)

        # An identifier which will resolve to the package just deposited
        deposit_uri = self.um.part_uri(collection, id, deposit.filename)

        # the Cont-URI
        content_uri = self.um.cont_uri(collection, id)

        # the Edit-URI
        edit_uri = self.um.edit_uri(collection, id)

        # create the initial statement
        s = Statement()
        s.aggregation_uri = content_uri
        s.rem_uri = edit_uri
        s.original_deposit(deposit_uri, datetime.now(), deposit.packaging)
        s.in_progress = deposit.in_progress

        # store the statement by itself
        self.dao.store_statement(collection, id, s)

        # create the deposit receipt (which involves getting hold of the item's metadata first if it exists
        metadata = self.dao.get_metadata(collection, id)
        receipt = self.deposit_receipt(collection, id, deposit, s, metadata)

        # store the deposit receipt also
        self.dao.store_deposit_receipt(collection, id, receipt)

        # finally, assemble the deposit response and return
        dr = DepositResponse()
        dr.receipt = receipt
        dr.location = edit_uri
        if deposit.in_progress:
            dr.accepted = True
        else:
            dr.created = True

        return dr

    def get_media_resource(self, oid, content_type):
        """
        Get a representation of the media resource for the given id as represented by the specified content type
        -id:    The ID of the object in the store
        -content_type   A ContentType object describing the type of the object to be retrieved
        """
        # by the time this is called, we should already know that we can return this type, so there is no need for
        # any checking, we just get on with it

        # ok, so break the id down into collection and object
        collection, id = self.um.interpret_oid(oid)

        # make a MediaResourceResponse object for us to use
        mr = MediaResourceResponse()

        # if the type/subtype is text/html, then we need to do a redirect.  This is equivalent to redirecting the
        # client to the splash page of the item on the server
        if content_type.mimetype() == "text/html":
            mr.redirect = True
            mr.url = self.um.html_url(collection, id)
            return mr

        # call the appropriate packager, and get back the filepath for the response
        packager = self.configuration.package_disseminators[content_type.mimetype()]()
        mr.filepath = packager.package(collection, id)

        return mr

    def replace(self, oid, deposit):
        """
        Replace all the content represented by the supplied id with the supplied deposit
        Args:
        - oid:  the object ID in the store
        - deposit:  a DepositRequest object
        Return a DepositResponse containing the Deposit Receipt or a SWORD Error
        """
        collection, id = self.um.interpret_oid(oid)

        # does the object directory exist?  If not, we can't do a deposit
        if not self.exists(oid):
            return None

        # remove all the old files before adding the new.  We leave behind the atom file if X-Suppress-Metadata is
        # supplied
        self.dao.remove_content(collection, id, deposit.suppress_metadata)

        # store the content file
        fn = self.dao.store_content(collection, id, deposit.content, deposit.filename)

        # now that we have stored the atom and the content, we can invoke a package ingester over the top to extract
        # all the metadata and any files we want.  Notice that we pass in the suppress_metadata flag, so the
        # packager won't overwrite the existing metadata if it isn't supposed to
        packager = self.configuration.package_ingesters[deposit.packaging]()
        packager.ingest(collection, id, fn, deposit.suppress_metadata)

        # An identifier which will resolve to the package just deposited
        deposit_uri = self.um.part_uri(collection, id, deposit.filename)

        # the Cont-URI
        content_uri = self.um.cont_uri(collection, id)

        # the Edit-URI
        edit_uri = self.um.edit_uri(collection, id)

        # create the new statement
        s = Statement()
        s.aggregation_uri = content_uri
        s.rem_uri = edit_uri
        s.original_deposit(deposit_uri, datetime.now(), deposit.packaging)
        s.in_progress = deposit.in_progress

        # store the statement by itself
        self.dao.store_statement(collection, id, s)

        # create the deposit receipt
        receipt = self.deposit_receipt(collection, id, deposit, s, None)

        # store the deposit receipt also
        self.dao.store_deposit_receipt(collection, id, receipt)

        # finally, assemble the deposit response and return
        dr = DepositResponse()
        dr.receipt = receipt
        if deposit.in_progress:
            dr.accepted = True
        else:
            dr.created = True

        return dr

    def delete_content(self, oid, delete):
        """
        Delete all of the content from the object identified by the supplied id.  the parameters of the delete
        request must also be supplied
        - oid:  The ID of the object to delete the contents of
        - delete:   The DeleteRequest object
        Return a DeleteResponse containing the Deposit Receipt or the SWORD Error
        """
        collection, id = self.um.interpret_oid(oid)

        # does the collection directory exist?  If not, we can't do a deposit
        if not self.exists(oid):
            return None

        # remove all the old files before adding the new.
        # notice that here we allow the metadata file to remain if requested in X-Suppress-Metadata.  This is a
        # question with regard to how the standard should work.
        self.dao.remove_content(collection, id, delete.suppress_metadata)

        # the Cont-URI
        content_uri = self.um.cont_uri(collection, id)

        # the Edit-URI
        edit_uri = self.um.edit_uri(collection, id)

        # create the statement
        s = Statement()
        s.aggregation_uri = content_uri
        s.rem_uri = edit_uri
        s.in_progress = delete.in_progress

        # store the statement by itself
        self.dao.store_statement(collection, id, s)

        # create the deposit receipt
        receipt = self.deposit_receipt(collection, id, delete, s, None)

        # store the deposit receipt also
        self.dao.store_deposit_receipt(collection, id, receipt)

        # finally, assemble the delete response and return
        dr = DeleteResponse()
        dr.receipt = receipt
        return dr

    def get_container(self, oid, content_type):
        """
        Get a representation of the container in the requested content type
        Args:
        -oid:   The ID of the object in the store
        -content_type   A ContentType object describing the required format
        Returns a representation of the container in the appropriate format
        """
        # by the time this is called, we should already know that we can return this type, so there is no need for
        # any checking, we just get on with it

        # ok, so break the id down into collection and object
        collection, id = self.um.interpret_oid(oid)

        # pick either the deposit receipt or the pure statement to return to the client
        if content_type.mimetype() == "application/atom+xml;type=entry":
            return self.dao.get_deposit_receipt_content(collection, id)
        elif content_type.mimetype() == "application/rdf+xml":
            return self.dao.get_statement_content(collection, id)

    def deposit_existing(self, oid, deposit):
        """
        Deposit the incoming content into an existing object as identified by the supplied identifier
        Args:
        -oid:   The ID of the object we are depositing into
        -deposit:   The DepositRequest object
        Returns a DepositResponse containing the Deposit Receipt or a SWORD Error
        """
        collection, id = self.um.interpret_oid(oid)

        # does the collection directory exist?  If not, we can't do a deposit
        if not self.exists(oid):
            return None

        # now just store the atom file and the content (this may overwrite an existing atom document - this is
        # intentional)
        if deposit.atom is not None:
            self.dao.store_atom(collection, id, deposit.atom)

        # store the content file
        fn = self.dao.store_content(collection, id, deposit.content, deposit.filename)

        # now that we have stored the atom and the content, we can invoke a package ingester over the top to extract
        # all the metadata and any files we want.  Notice that we pass in the suppress_metadata flag, so the packager
        # won't overwrite the metadata if it isn't supposed to
        packager = self.configuration.package_ingesters[deposit.packaging]()
        packager.ingest(collection, id, fn, deposit.suppress_metadata)

        # An identifier which will resolve to the package just deposited
        deposit_uri = self.um.part_uri(collection, id, deposit.filename)

        # load the statement
        s = self.dao.load_statement(collection, id)

        # add the new deposit
        s.original_deposit(deposit_uri, datetime.now(), deposit.packaging)
        s.in_progress = deposit.in_progress

        # store the statement by itself
        self.dao.store_statement(collection, id, s)

        # create the deposit receipt
        receipt = self.deposit_receipt(collection, id, deposit, s, None)

        # store the deposit receipt also
        self.dao.store_deposit_receipt(collection, id, receipt)

        # finally, assemble the deposit response and return
        dr = DepositResponse()
        dr.receipt = receipt
        if deposit.in_progress:
            dr.accepted = True
        else:
            dr.created = True

        return dr

    def delete_container(self, oid, delete):
        """
        Delete the entire object in the store
        Args:
        -oid:   The ID of the object in the store
        -delete:    The DeleteRequest object
        Return a DeleteResponse object with may contain a SWORD Error document or nothing at all
        """
        collection, id = self.um.interpret_oid(oid)

        # does the collection directory exist?  If not, we can't do a deposit
        if not self.exists(oid):
            return None

        # request the deletion of the container
        self.dao.remove_container(collection, id)
        return DeleteResponse()

    def deposit_receipt(self, collection, id, deposit, statement, metadata):
        """
        Construct a deposit receipt document for the provided URIs
        Args:
        -deposit_id:    The Atom Entry ID to use
        -cont_uri:   The Cont-URI from which the media resource content can be retrieved
        -em_uri:    The EM-URI (Edit Media) at which operations on the media resource can be conducted
        -edit_uri:  The Edit-URI at which operations on the container can be conducted
        -statement: A Statement object to be embedded in the receipt as foreign markup
        Returns a string representation of the deposit receipt
        """
        # assemble the URIs we are going to need

        # the atom entry id
        drid = self.um.atom_id(collection, id)

        # the Cont-URI
        cont_uri = self.um.cont_uri(collection, id)

        # the EM-URI
        em_uri = self.um.em_uri(collection, id)

        # the Edit-URI
        edit_uri = self.um.edit_uri(collection, id)

        # ensure that there is a metadata object, and that it is populated with enough information to build the
        # deposit receipt
        if metadata is None:
            metadata = {}
        if not metadata.has_key("title"):
            metadata["title"] = "SWORD Deposit"
        if not metadata.has_key("creator"):
            metadata["creator"] = "SWORD Client"
        if not metadata.has_key("abstract"):
            metadata["abstract"] = "Conten deposited with SWORD client"

        # Now assemble the deposit receipt

        # the main entry document room
        entry = etree.Element(self.ns.ATOM + "entry", nsmap=self.drmap)

        # Title from metadata
        title = etree.SubElement(entry, self.ns.ATOM + "title")
        title.text = metadata['title']

        # Atom Entry ID
        id = etree.SubElement(entry, self.ns.ATOM + "id")
        id.text = drid

        # Date last updated (i.e. NOW)
        updated = etree.SubElement(entry, self.ns.ATOM + "updated")
        updated.text = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        # Author field from metadata
        author = etree.SubElement(entry, self.ns.ATOM + "author")
        name = etree.SubElement(author, self.ns.ATOM + "name")
        name.text = metadata['creator']

        # Summary field from metadata
        summary = etree.SubElement(entry, self.ns.ATOM + "summary")
        summary.set("type", "text")
        summary.text = metadata['abstract']

        # User Agent
        # FIXME: why is the userAgent relevant?  Can we get rid of it?  I have done so for the purposes of example
        #userAgent = etree.SubElement(entry, self.ns.SWORD + "userAgent")
        #userAgent.text = "MyJavaClient/0.1 Restlet/2.0"

        # Generator - identifier for this server software
        generator = etree.SubElement(entry, self.ns.ATOM + "generator")
        generator.set("uri", "http://www.swordapp.org/sss")
        generator.set("version", "1.0")

        # Media Resource Content URI (Cont-URI)
        # FIXME: there is some uncertainty here over what the type value should be.  See notes.
        content = etree.SubElement(entry, self.ns.ATOM + "content")
        content.set("type", "application/zip")
        content.set("src", cont_uri)

        # FIXME: this is a proposal for how to possibly deal with announcing which package formats
        # can be content negotiated for.  Basically we allow for multiple sword:package elements
        # which will tell people what they can content negotiate for
        for disseminator in self.configuration.sword_disseminate_package:
            sp = etree.SubElement(entry, self.ns.SWORD + "package")
            sp.text = disseminator

        # Edit-URI
        editlink = etree.SubElement(entry, self.ns.ATOM + "link")
        editlink.set("rel", "edit")
        editlink.set("href", edit_uri)

        # EM-URI (Media Resource)
        emlink = etree.SubElement(entry, self.ns.ATOM + "link")
        emlink.set("rel", "edit-media")
        emlink.set("href", em_uri)

        # now finally embed the statement as foreign markup and return
        xml = statement.get_xml()
        entry.append(xml)
        return etree.tostring(entry, pretty_print=True)

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
        """
        self.aggregation_uri = None
        self.rem_uri = None
        self.original_deposits = []
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

    def __str__(self):
        return str(self.aggregation_uri) + ", " + str(self.rem_uri) + ", " + str(self.original_deposits)
        
    def original_deposit(self, uri, deposit_time, packaging_format):
        """
        Add an original deposit to the statement
        Args:
        - uri:  The URI to the original deposit
        - deposit_time:     When the deposit was originally made
        - packaging_format:     The package format of the deposit, as supplied in the X-Packaging header
        """
        self.original_deposits.append((uri, deposit_time, packaging_format))

    def load(self, filepath):
        """
        Populate this statement object from the XML serialised statement to be found at the specified filepath
        """
        f = open(filepath, "r")
        rdf = etree.fromstring(f.read())

        for desc in rdf.getchildren():
            packaging = None
            depositedOn = None
            about = desc.get(self.ns.RDF + "about")
            for element in desc.getchildren():
                if element.tag == self.ns.ORE + "describes":
                    resource = element.get(self.ns.RDF + "resource")
                    self.aggregation_uri = about
                    self.rem_uri = resource
                if element.tag == self.ns.SWORD + "state":
                    state = element.get(self.ns.RDF + "resource")
                    self.in_progress = state == "http://purl.org/net/sword/state/in-progress"
                if element.tag == self.ns.SWORD + "packaging":
                    packaging = element.get(self.ns.RDF + "resource")
                if element.tag == self.ns.SWORD + "depositedOn":
                    deposited = element.text
                    depositedOn = datetime.strptime(deposited, "%Y-%m-%dT%H:%M:%SZ")
            if packaging is not None:
                self.original_deposit(about, depositedOn, packaging)

    def serialise(self):
        """
        Serialise this statement into an RDF/XML string
        """
        rdf = self.get_xml()
        return etree.tostring(rdf, pretty_print=True)

    def get_xml(self):
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

        # Create ore:aggregates and sword:originalDeposit relations for the original deposits
        for (uri, datestamp, format) in self.original_deposits:
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
        for (uri, datestamp, format_uri) in self.original_deposits:
            desc = etree.SubElement(rdf, self.ns.RDF + "Description")
            desc.set(self.ns.RDF + "about", uri)

            format = etree.SubElement(desc, self.ns.SWORD + "packaging")
            format.set(self.ns.RDF + "resource", format_uri)

            deposited = etree.SubElement(desc, self.ns.SWORD + "depositedOn")
            deposited.set(self.ns.RDF + "datatype", "http://www.w3.org/2001/XMLSchema#dateTime")
            deposited.text = datestamp.strftime("%Y-%m-%dT%H:%M:%SZ")

        # finally do a description for the state
        sdesc = etree.SubElement(rdf, self.ns.RDF + "Description")
        sdesc.set(self.ns.RDF + "about", state_uri)
        meaning = etree.SubElement(sdesc, self.ns.SWORD + "stateDescription")
        meaning.text = self.states[state_uri]

        return rdf

class URIManager(object):
    """
    Class for providing a single point of access to all identifiers used by SSS
    """
    def __init__(self):
        self.configuration = Configuration()

    def html_url(self, collection, id):
        """ The url for the HTML splash page of an object in the store """
        return self.configuration.base_url + "html/" + collection + "/" + id

    def col_uri(self, id):
        """ The url for a collection on the server """
        return self.configuration.base_url + "col-uri/" + id

    def edit_uri(self, collection, id):
        """ The Edit-URI """
        return self.configuration.base_url + "edit-uri/" + collection + "/" + id

    def em_uri(self, collection, id):
        """ The EM-URI """
        return self.configuration.base_url + "em-uri/" + collection + "/" + id

    def cont_uri(self, collection, id):
        """ The Cont-URI """
        return self.configuration.base_url + "cont-uri/" + collection + "/" + id

    def part_uri(self, collection, id, filename):
        """ The URL for accessing the parts of an object in the store """
        return self.configuration.base_url + "part-uri/" + collection + "/" + id + "/" + filename

    def atom_id(self, collection, id):
        """ An ID to use for Atom Entries """
        return "tag:container@sss/" + collection + "/" + id

    def interpret_oid(self, oid):
        """
        Take an object id from a URL and interpret the collection and id terms.
        Returns a tuple of (collection, id)
        """
        collection, id = oid.split("/", 1)
        return collection, id

class DAO(object):
    """
    Data Access Object for interacting with the store
    """
    def __init__(self):
        """
        Initialise the DAO.  This creates the store directory in the Configuration() object if it does not already
        exist and will construct the relevant number of fake collections.  In general if you make changes to the
        number of fake collections you want to have, it's best just to burn the store and start from scratch, although
        this method will check to see that it has enough fake collections and make up the defecit, but it WILL NOT
        remove excess collections
        """
        self.configuration = Configuration()

        # first thing to do is create the store if it does not already exist
        if not os.path.exists(self.configuration.store_dir):
            os.makedirs(self.configuration.store_dir)

        # now construct the fake collections
        current_cols = os.listdir(self.configuration.store_dir)
        create = self.configuration.num_collections - len(current_cols)
        for i in range(create):
            name = str(uuid.uuid4())
            cdir = os.path.join(self.configuration.store_dir, name)
            os.makedirs(cdir)

        self.ns = Namespaces()
        self.mdmap = {None : self.ns.DC_NS}

    def get_collection_names(self):
        """ list all the collections in the store """
        return os.listdir(self.configuration.store_dir)

    def collection_exists(self, collection):
        """
        Does the specified collection exist?
        Args:
        -collection:    the Collection name
        Returns true or false
        """
        cdir = os.path.join(self.configuration.store_dir, collection)
        return os.path.exists(cdir)

    def container_exists(self, collection, id):
        """
        Does the specified container exist?  If the collection does not exist this will still return and will return
        false
        Args:
        -collection:    the Collection name
        -id:    the container id
        Returns true or false
        """
        odir = os.path.join(self.configuration.store_dir, collection, id)
        return os.path.exists(odir)

    def file_exists(self, collection, id, filename):
        fpath = os.path.join(self.configuration.store_dir, collection, id, filename)
        return os.path.exists(fpath)

    def create_container(self, collection):
        """
        Create a container in the specified collection.  The container will be assigned a random UUID as its
        identifier.
        Args:
        -collection:    the collection name in which to create the container
        Returns the ID of the container
        """
        # invent an identifier for the item, and create its directory
        id = str(uuid.uuid4())
        odir = os.path.join(self.configuration.store_dir, collection, id)
        if not os.path.exists(odir):
            os.makedirs(odir)
        return id

    def save(self, filepath, content, opts="w"):
        """
        Shortcut to save the content to the filepath with the associated file handle opts (defaults to "w", so pass
        in "wb" for binary files
        """
        f = open(filepath, opts)
        f.write(content)
        f.close()

    def get_filename(self, filename):
        """
        Create a timestamped file name to avoid name clashes in the store
        """
        return datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ") + "_" + filename

    def store_atom(self, collection, id, atom):
        """ Store the supplied atom document content in the object identified by the id in the specified collection """
        afile = os.path.join(self.configuration.store_dir, collection, id, "atom.xml")
        self.save(afile, atom)

    def store_content(self, collection, id, content, filename):
        """
        Store the supplied content in the object identified by the id in the specified collection under the supplied
        filename.  In reality, to avoid name colisions the filename will be preceeded with a timestamp in the store.
        Returns the localised filename the content was stored under
        """
        ufn = self.get_filename(filename)
        cfile = os.path.join(self.configuration.store_dir, collection, id, ufn)
        self.save(cfile, content, "wb")
        return ufn

    def store_statement(self, collection, id, statement):
        """ Store the supplied statement document content in the object idenfied by the id in the specified collection """
        sfile = os.path.join(self.configuration.store_dir, collection, id, "sss_statement.xml")
        self.save(sfile, statement.serialise())

    def store_deposit_receipt(self, collection, id, receipt):
        """ Store the supplied receipt document content in the object idenfied by the id in the specified collection """
        drfile = os.path.join(self.configuration.store_dir, collection, id, "sss_deposit-receipt.xml")
        self.save(drfile, receipt)

    def store_metadata(self, collection, id, metadata):
        """ Store the supplied metadata dictionary in the object idenfied by the id in the specified collection """
        md = etree.Element(self.ns.DC + "metadata", nsmap=self.mdmap)
        for dct in metadata.keys():
            element = etree.SubElement(md, self.ns.DC + dct)
            element.text = metadata[dct]
        s = etree.tostring(md, pretty_print=True)
        mfile = os.path.join(self.configuration.store_dir, collection, id, "sss_metadata.xml")
        self.save(mfile, s)

    def get_metadata(self, collection, id):
        if not self.file_exists(collection, id, "sss_metadata.xml"):
            return {}
        mfile = os.path.join(self.configuration.store_dir, collection, id, "sss_metadata.xml")
        f = open(mfile, "r")
        metadata = etree.fromstring(f.read())
        md = {}
        for dc in metadata.getchildren():
            tag = dc.tag
            if tag.startswith(self.ns.DC):
                tag = tag[len(self.ns.DC):]
            md[tag] = dc.text.strip()
        return md

    def remove_content(self, collection, id, keep_metadata=False):
        """
        Remove all the content from the specified container.  If keep_metadata is True then the sss_metadata.xml
        file will not be removed
        """
        odir = os.path.join(self.configuration.store_dir, collection, id)
        for file in os.listdir(odir):
            # if there is a metadata.xml but metadata suppression on the deposit is turned on
            # then leave it alone
            if file == "sss_metadata.xml" and keep_metadata:
                continue
            dpath = os.path.join(odir, file)
            os.remove(dpath)

    def remove_container(self, collection, id):
        """ Remove the specified container and all of its contents """

        # first remove the contents of the container
        self.remove_content(collection, id)

        # finally remove the container itself
        odir = os.path.join(self.configuration.store_dir, collection, id)
        os.rmdir(odir)

    def get_store_path(self, collection, id, filename):
        """
        Get the path to the specified filename in the store.  This is a utility method and should be used with care;
        all content which goes into the store through the store_content method will have its filename localised to
        avoid name clashes, so this method CANNOT be used to retrieve those files.  Instead, this should be used
        internally to locate sss specific files in the container, and for packagers to write their own files into
        the store which are not part of the content itself.
        """
        fpath = os.path.join(self.configuration.store_dir, collection, id, filename)
        return fpath

    def get_deposit_receipt_content(self, collection, id):
        """ Read the deposit receipt for the specified container """
        f = open(self.get_store_path(collection, id, "sss_deposit-receipt.xml"), "r")
        return f.read()

    def get_statement_content(self, collection, id):
        """ Read the statement for the specified container """
        f = open(self.get_store_path(collection, id, "sss_statement.xml"), "r")
        return f.read()

    def get_atom_content(self, collection, id):
        """ Read the statement for the specified container """
        if not self.file_exists(collection, id, "atom.xml"):
            return None
        f = open(self.get_store_path(collection, id, "atom.xml"), "r")
        return f.read()

    def load_statement(self, collection, id):
        """
        Load the Statement object for the specified container
        Returns a Statement object fully populated to represent this object
        """
        sfile = os.path.join(self.configuration.store_dir, collection, id, "sss_statement.xml")
        s = Statement()
        s.load(sfile)
        return s

    def list_content(self, collection, id, exclude=[]):
        """
        List the contents of the specified container, excluding any files whose name exactly matches those in the
        exclude list.  This method will also not list sss specific files, thus limiting it to the content files of
        the object.
        """
        cdir = os.path.join(self.configuration.store_dir, collection)
        odir = os.path.join(cdir, id)
        cfiles = [f for f in os.listdir(odir) if not f.startswith("sss_") and not f in exclude]
        return cfiles

# DISSEMINATION PACKAGING
#######################################################################
# This section contains a Packager interface and classes which provide dissemination packaging for the SSS
# Packagers can be configured in the Configuration object to be called for requested content types

class DisseminationPackager(object):
    """
    Interface for all classes wishing to provide dissemination packaging services to the SSS
    """
    def package(self, collection, id):
        """
        Package up all the content in the specified container.  This method must be implemented by the extender.  The
        method should create a package in the store directory, and then return to the caller the path to that file
        so that it can be served back to the client
        """
        pass

class DefaultDisseminator(DisseminationPackager):
    """
    Basic default packager, this just zips up everything except the SSS specific files in the container and stores
    them in a file called sword-default-package.zip.
    """
    def __init__(self):
        self.dao = DAO()

    def package(self, collection, id):
        """ package up the content """

        # get a list of the relevant content files
        files = self.dao.list_content(collection, id, exclude=["sword-default-package.zip"])

        # create a zip file with all the original zip files in it
        zpath = self.dao.get_store_path(collection, id, "sword-default-package.zip")
        z = ZipFile(zpath, "w")
        for file in files:
            z.write(self.dao.get_store_path(collection, id, file), file)
        z.close()

        # return the path to the package to the caller
        return zpath

class IngestPackager(object):
    def ingest(self, collection, id, filename, suppress_metadata):
        """
        The package with the supplied filename has been placed in the identified container.  This should be inspected
        and unpackaged.  Implementations should note that there is optionally an atom document in the container which
        may need to be inspected, and this can be retrieved from DAO.get_atom_content().  If the suppress_metadata
        argument is True, implementations should not change the already extracted metadata in the container
        """
        pass

class DefaultIngester(IngestPackager):
    def __init__(self):
        self.dao = DAO()
        self.ns = Namespaces()
        
    def ingest(self, collection, id, filename, suppress_metadata):
        # for the time being this is just going to generate the metadata, it won't bother unpacking the zip
        
        # check for the atom document
        atom = self.dao.get_atom_content(collection, id)
        if atom is None:
            # there's no metadata to extract so just leave it
            return

        metadata = {}
        entry = etree.fromstring(atom)

        # go through each element in the atom entry and just process the ones we care about
        # explicitly retrieve the atom based metadata first, then we'll overwrite it later with
        # the dcterms metadata where appropriate
        for element in entry.getchildren():
            if element.tag == self.ns.ATOM + "title":
                metadata["title"] = element.text.strip()
            if element.tag == self.ns.ATOM + "updated":
                metadata["date"] = element.text.strip()
            if element.tag == self.ns.ATOM + "author":
                authors = ""
                for names in element.getchildren():
                    authors += names.text.strip() + " "
                metadata["creator"] = authors
            if element.tag == self.ns.ATOM + "summary":
                metadata["abstract"] = element.text.strip()

        # now go through and retrieve the dcterms from the entry
        for element in entry.getchildren():
            if not isinstance(element.tag, basestring):
                continue
                
            if element.tag.startswith(self.ns.DC):
                metadata[element.tag[len(self.ns.DC):]] = element.text.strip()

        self.dao.store_metadata(collection, id, metadata)

class METSDSpaceIngester(IngestPackager):
    def ingest(self, collection, id, filename, suppress_metadata):
        # we don't need to implement this, it is just for example.  it would unzip the file and import the metadata
        # in the zip file
        pass

# WEB SERVER
#######################################################################
# This is the bit which actually invokes the web.py server when this module is run

app = web.application(urls, globals())
if __name__ == "__main__":
    app.run()