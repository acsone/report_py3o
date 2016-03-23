# -*- encoding: utf-8 -*-
import ast
from cStringIO import StringIO
import collections
import datetime
from functools import partial
from itertools import imap
import json
import pkg_resources
import os
import sys
from base64 import b64decode
import requests
from tempfile import NamedTemporaryFile

from openerp import _
from openerp import exceptions
from openerp.osv.orm import browse_record
from openerp.osv.orm import browse_record_list
from openerp.report.report_sxw import report_sxw, rml_parse
from openerp import registry
from openerp.exceptions import ValidationError

from py3o.template.helpers import Py3oConvertor
from py3o.template import Template

from py3o.types import Py3oTypeConfig
from py3o.types import Py3oInteger
from py3o.types import Py3oFloat
from py3o.types import Py3oDate
from py3o.types import Py3oTime
from py3o.types import Py3oDatetime
from py3o.types.json import Py3oJSONEncoder
from py3o.types.json import Py3oJSONDecoder

_extender_functions = {}


class TemplateNotFound(Exception):
    pass


class OpenERPToPy3o(collections.Mapping, collections.Sequence):

    openerp_py3o_types = {
        'integer': Py3oInteger,
        'float': Py3oFloat,
        'date': partial(Py3oDate.strptime, format='%Y-%m-%d'),
        'time': partial(Py3oTime.strptime, format='%H:%M:%S'),
        'datetime': partial(Py3oDatetime.strptime, format='%Y-%m-%d %H:%M:%S'),
    }

    default_py3o_types = {
        int: Py3oInteger,
        float: Py3oFloat,
        datetime.date: Py3oDate,
        datetime.time: Py3oTime,
        datetime.datetime: Py3oDatetime,
    }

    @classmethod
    def from_config(cls, config):

        oetypes = {
            'integer': config.integer,
            'float': config.float,
            'date': partial(config.date.strptime, format='%Y-%m-%d'),
            'time': partial(config.time.strptime, format='%H:%M:%S'),
            'datetime': partial(
                config.datetime.strptime, format='%Y-%m-%d %H:%M:%S'
            ),
        }

        deftypes = {
            int: config.integer,
            float: config.float,
            datetime.date: config.date,
            datetime.time: config.time,
            datetime.datetime: config.datetime
        }

        nmspc = {
            'openerp_py3o_types': oetypes, 'default_py3o_types': deftypes
        }
        return type('TEST'+cls.__name__, (cls,), nmspc)

    def __init__(self, cr, uid, value, fields_get=None):
        self._cr = cr
        self._uid = uid
        self._value = value
        self._fields_get = fields_get

    def __getitem__(self, item):
        return self._to_py3o(item, self._value[item])

    def __getattr__(self, attr):
        return self._to_py3o(attr, getattr(self._value, attr))

    def _to_py3o(self, key, val):
        cr, uid, fields_get = self._cr, self._uid, self._fields_get
        field = fields_get.get(key, None) if fields_get else None

        # Order is important for v8 : all browse_record are browse_record_list
        if isinstance(val, browse_record):
            if field is None:
                field = {}
                if fields_get is not None:
                    fields_get[key] = field
            if 'id' not in field:
                field.update(val._model.fields_get(cr, uid))
            res = type(self)(cr, uid, val, field)

        elif isinstance(val, browse_record_list):
            res = [self._to_py3o(key, v) for v in val]

        elif isinstance(val, (dict, list, tuple)):
            # Lose the fields_get context
            res = type(self)(cr, uid, val)

        else:
            ftype = field.get('type', None) if field else None
            py3o_type = self.openerp_py3o_types.get(ftype) if ftype else None
            if py3o_type is None:
                py3o_type = self.default_py3o_types.get(type(val))
            res = py3o_type(val) if py3o_type is not None else val

        return res

    def __len__(self):
        return len(self._value)

    def __iter__(self):
        """Return an iterator for the wrapped value.

        If the value is a browse_record, return self inside an iterable.
        If it is a list or a browse_record_list, return an iterator that

        This is the desired behavior for a dictionary.
        """

        if isinstance(self._value, browse_record):
            return iter((self,))
        elif isinstance(self._value, (list, browse_record_list)):
            return imap(self._to_py3o, imap(enumerate, self._value))
        else:
            return iter(self._value)


def py3o_report_extender(report_name):
    """
    A decorator to define function to extend the context sent to a template.
    This will be called at the creation of the report.
    The following arguments will be passed to it:
        - pool: the model pool
        - cr: the database cursor
        - uid: the id of the user that call the renderer
        - localcontext: The context that will be passed to the report engine
        - context: the Odoo context

    Method copied from CampToCamp report_webkit module.

    :param report_name: xml id of the report
    :return: a decorated class
    """
    def fct1(fct):
        lst = _extender_functions.get(report_name)
        if not lst:
            lst = []
            _extender_functions[report_name] = lst
        lst.append(fct)
        return fct
    return fct1


class Py3oParser(report_sxw):
    """Custom class that use Py3o to render libroffice reports.
        Code partially taken from CampToCamp's webkit_report."""

    def __init__(self, name, table, rml=False, parser=rml_parse,
                 header=False, store=False, register=True):
        self.localcontext = {}
        super(Py3oParser, self).__init__(
            name, table, rml=rml, parser=parser,
            header=header, store=store, register=register
        )

    def get_template(self, report_obj):
        """private helper to fetch the template data either from the database
        or from the default template file provided by the implementer.

        ATM this method takes a report definition recordset
        to try and fetch the report template from database. If not found it will
        fallback to the template file referenced in the report definition.

        @param report_obj: a recordset representing the report defintion
        @type report_obj: openerp.model.recordset instance

        @returns: string or buffer containing the template data

        @raises: TemplateNotFound which is a subclass of
        openerp.exceptions.DeferredException
        """

        tmpl_data = None

        if report_obj.py3o_template_id and report_obj.py3o_template_id.id:
            # if a user gave a report template
            tmpl_data = b64decode(
                report_obj.py3o_template_id.py3o_template_data
            )

        elif report_obj.py3o_template_fallback and report_obj.module:
            # if the default is defined
            flbk_filename = pkg_resources.resource_filename(
                "openerp.addons.%s" % report_obj.module,
                report_obj.py3o_template_fallback,
            )
            if os.path.exists(flbk_filename):
                # and it exists on the fileystem
                with open(flbk_filename, 'r') as tmpl:
                    tmpl_data = tmpl.read()

        if tmpl_data is None:
            # if for any reason the template is not found
            raise TemplateNotFound(
                _(u'No template found. Aborting.'),
                sys.exc_info(),
            )

        return tmpl_data

    def get_default_type_config(self, cr, uid, pool=None, context=None):

        if pool is None:
            pool = registry(cr.dbname)
        lang_osv = pool['res.lang']

        if context and 'lang' in context:
            lang = context['lang']
        else:
            lang = pool['res.users'].browse(cr, uid, uid, context=context).lang

        lang_domain = [('code', '=', lang)]
        lang_id = lang_osv.search(cr, uid, lang_domain, context=context)
        if not lang_id:
            return None

        lang_rec = lang_osv.browse(cr, uid, lang_id, context=context)
        res = {
            'date_format': lang_rec.date_format,
            'time_format': lang_rec.time_format,
            'digit_separator': lang_rec.thousands_sep,
            'decimal_separator': lang_rec.decimal_point,
        }

        try:
            grouping = ast.literal_eval(lang_rec.grouping)
            if grouping:
                res['digit_format'] = grouping[0]
        except (SyntaxError, ValueError):
            pass

        return res

    def create_single_pdf(self, cr, uid, ids, data, report_xml, context=None):
        """ Overide this function to generate our py3o report
        """
        if report_xml.report_type != 'py3o':
            return super(Py3oParser, self).create_single_pdf(
                cr, uid, ids, data, report_xml, context=context
            )

        pool = registry(cr.dbname)
        model_data_ids = pool['ir.model.data'].search(
            cr, uid, [
                ('model', '=', 'ir.actions.report.xml'),
                ('res_id', '=', report_xml.id),
            ]
        )

        xml_id = None
        if model_data_ids:
            model_data = pool['ir.model.data'].browse(
                cr, uid, model_data_ids[0], context=context
            )
            xml_id = '%s.%s' % (model_data.module, model_data.name)

        parser_instance = self.parser(cr, uid, self.name2, context=context)
        parser_instance.set_context(
            self.getObjects(cr, uid, ids, context),
            data, ids, report_xml.report_type
        )

        if xml_id in _extender_functions:
            for fct in _extender_functions[xml_id]:
                fct(pool, cr, uid, parser_instance.localcontext, context)

        tmpl_data = self.get_template(report_xml)

        in_stream = StringIO(tmpl_data)
        out_stream = StringIO()
        template = Template(in_stream, out_stream)
        expressions = template.get_all_user_python_expression()
        py_expression = template.convert_py3o_to_python_ast(expressions)
        convertor = Py3oConvertor()
        data_struct = convertor(py_expression)

        filetype = report_xml.py3o_fusion_filetype

        type_config_defaults = self.get_default_type_config(
            cr, uid, pool=pool, context=context
        )
        type_config = Py3oTypeConfig.from_odf_file(
            in_stream, defaults=type_config_defaults
        )

        datadict = parser_instance.localcontext
        py3o_translator = OpenERPToPy3o.from_config(type_config)
        translator_datadict = py3o_translator(cr, uid, datadict)
        parsed_datadict = data_struct.render(translator_datadict)

        fusion_server_obj = pool.get('py3o.server')
        fusion_server_ids = fusion_server_obj.search(
            cr, uid, [('is_active', '=', True)], context=context, limit=1
        )
        if not fusion_server_ids:
            if filetype.fusion_ext == report_xml.py3o_template_id.filetype:
                # No format conversion is needed, render the template directly
                template.render(parsed_datadict)
                res = out_stream.getvalue()
            else:
                raise exceptions.MissingError(
                    _(u"No Py3o server configuration found")
                )

        else:  # Call py3o.server to render the template in the desired format
            fusion_server_id = fusion_server_ids[0]

            fusion_server = fusion_server_obj.browse(
                cr, uid, fusion_server_id, context=context
            )

            # Update the template metadata with the used configuration
            files = {
                'tmpl_file': type_config.apply_to_odf_file(in_stream)
            }
            fields = {
                "targetformat": filetype.fusion_ext,
                "datadict": Py3oJSONEncoder().encode(parsed_datadict),
                "image_mapping": "{}",
            }
            r = requests.post(fusion_server.url, data=fields, files=files)
            if r.status_code != 200:
                # server says we have an issue... let's tell that to enduser
                raise exceptions.Warning(
                    _('Fusion server error'),
                    r.text,
                )

            # Here is a little joke about Odoo
            # we do nice chunked reading from the network...
            chunk_size = 1024
            with NamedTemporaryFile(
                    suffix=filetype.human_ext,
                    prefix='py3o-template-'
            ) as fd:
                for chunk in r.iter_content(chunk_size):
                    fd.write(chunk)
                fd.seek(0)
                # ... but odoo wants the whole data in memory anyways :)
                res = fd.read()

        return res, filetype.human_ext

    def create(self, cr, uid, ids, data, context=None):
        """ Override this function to handle our py3o report
        """
        pool = registry(cr.dbname)
        ir_action_report_obj = pool['ir.actions.report.xml']
        report_xml_ids = ir_action_report_obj.search(
            cr, uid, [('report_name', '=', self.name[7:])], context=context
        )
        if not report_xml_ids:
            return super(Py3oParser, self).create(
                cr, uid, ids, data, context=context
            )

        report_xml = ir_action_report_obj.browse(
            cr, uid, report_xml_ids[0], context=context
        )

        result = self.create_source_pdf(
            cr, uid, ids, data, report_xml, context
        )

        if not result:
            return False, False
        return result
