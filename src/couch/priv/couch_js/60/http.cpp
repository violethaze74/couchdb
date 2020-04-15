// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#include <algorithm>
#include <sstream>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <jsapi.h>
#include <js/Initialization.h>
#include "config.h"
#include "util.h"

// Soft dependency on cURL bindings because they're
// only used when running the JS tests from the
// command line which is rare.
#ifndef HAVE_CURL

void
http_check_enabled()
{
    fprintf(stderr, "HTTP API was disabled at compile time.\n");
    exit(3);
}


bool
http_ctor(JSContext* cx, JSObject* req)
{
    return false;
}


void
http_dtor(JSFreeOp* fop, JSObject* req)
{
    return;
}


bool
http_open(JSContext* cx, JSObject* req, JS::Value mth, JS::Value url, JS::Value snc)
{
    return false;
}


bool
http_set_hdr(JSContext* cx, JSObject* req, JS::Value name, JS::Value val)
{
    return false;
}


bool
http_send(JSContext* cx, JSObject* req, JS::Value body)
{
    return false;
}


int
http_status(JSContext* cx, JSObject* req)
{
    return -1;
}

bool
http_uri(JSContext* cx, JSObject* req, couch_args* args, JS::Value* uri_val)
{
    return false;
}


#else
#include <curl/curl.h>
#ifndef XP_WIN
#include <unistd.h>
#endif


// Map some of the string function names to things which exist on Windows
#ifdef XP_WIN
#define strcasecmp _strcmpi
#define strncasecmp _strnicmp
#define snprintf _snprintf
#endif

#define USER_AGENT "CouchHTTP Client - Relax"


class HTTP {
    public:
        HTTP();
        ~HTTP();

        bool set_method(std::string method);
        bool set_url(std::string url);
        bool add_header(std::string name, std::string value);
        void clear_headers();

        bool send(JSContext* cx, JSObject* req, std::string& body);
        int get_last_status();

        size_t send_body(void* ptr, size_t size, size_t nmem);
        int seek_body(curl_off_t offset, int origin);
        size_t recv_header(void* ptr, size_t size, size_t nmem);
        size_t recv_body(void* ptr, size_t size, size_t nmem);

    private:
        std::string         _method;
        std::string         _url;
        curl_slist*         _req_headers;
        int16_t             _last_status;

        JSContext*          _cx;
        JSObject*           _resp_headers;
        std::string         _body;
        size_t              _sent;
        int                 _sent_once;
        std::ostringstream  _recvbuf;
};


static size_t
send_body(void* data, size_t size, size_t nmem, void* userp)
{
    HTTP* http = static_cast<HTTP*>(userp);
    return http->send_body(data, size, nmem);
}


static int
seek_body(void* userp, curl_off_t offset, int origin)
{
    HTTP* http = static_cast<HTTP*>(userp);
    return http->seek_body(offset, origin);
}


static size_t
recv_header(void* data, size_t size, size_t nmem, void* userp)
{
    HTTP* http = static_cast<HTTP*>(userp);
    return http->recv_header(data, size, nmem);
}


static size_t
recv_body(void* data, size_t size, size_t nmem, void* userp)
{
    HTTP* http = static_cast<HTTP*>(userp);
    return http->recv_body(data, size, nmem);
}

/*
 * I really hate doing this but this doesn't have to be
 * uber awesome, it just has to work.
 */
CURL*       HTTP_HANDLE = NULL;
char        ERRBUF[CURL_ERROR_SIZE];

void
http_check_enabled()
{
    if(HTTP_HANDLE != NULL) {
        return;
    }

    HTTP_HANDLE = curl_easy_init();

    if(!HTTP_HANDLE) {
        fprintf(stderr, "Failed to initialize cURL handle.\n");
        exit(3);
    }

    curl_easy_setopt(HTTP_HANDLE, CURLOPT_READFUNCTION, send_body);
    curl_easy_setopt(HTTP_HANDLE, CURLOPT_SEEKFUNCTION, seek_body);
    curl_easy_setopt(HTTP_HANDLE, CURLOPT_HEADERFUNCTION, recv_header);
    curl_easy_setopt(HTTP_HANDLE, CURLOPT_WRITEFUNCTION, recv_body);
    curl_easy_setopt(HTTP_HANDLE, CURLOPT_NOPROGRESS, 1);
    curl_easy_setopt(HTTP_HANDLE, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);
    curl_easy_setopt(HTTP_HANDLE, CURLOPT_ERRORBUFFER, ERRBUF);
    curl_easy_setopt(HTTP_HANDLE, CURLOPT_COOKIEFILE, "");
    curl_easy_setopt(HTTP_HANDLE, CURLOPT_USERAGENT, USER_AGENT);
}


HTTP::HTTP() : _req_headers(nullptr)
{
}


HTTP::~HTTP()
{
    if(_req_headers != nullptr) {
        curl_slist_free_all(_req_headers);
    }
}


bool
HTTP::set_method(std::string method)
{
    if(!method.size()) {
        return false;
    }

    std::string allowed[] = {
        "GET",
        "HEAD",
        "POST",
        "PUT",
        "DELETE",
        "COPY",
        "OPTIONS"
    };

    std::for_each(method.begin(), method.end(), [](char & c) {
		c = ::toupper(c);
	});

    auto iter = std::find(std::begin(allowed), std::end(allowed), method);
    if(iter == std::end(allowed)) {
        return false;
    }

    _method = method;
    return true;
}


bool
HTTP::set_url(std::string url)
{
    if(!url.size()) {
        return false;
    }

    _url = url;
    return true;
}


bool
HTTP::add_header(std::string name, std::string value)
{
    if(!name.size()) {
        return false;
    }

    std::string hdr = name + ": " + value;
    _req_headers = curl_slist_append(_req_headers, hdr.c_str());

    return true;
}


void
HTTP::clear_headers()
{
    if(_req_headers != nullptr) {
        curl_slist_free_all(_req_headers);
    }

    _req_headers = nullptr;
}


bool
HTTP::send(JSContext* cx, JSObject* req, std::string& body)
{
    _cx = cx;
    _resp_headers = NULL;
    _body = body;
    _sent = 0;
    _sent_once = 0;
    _recvbuf.clear();

    JS::RootedObject robj(cx, req);

    curl_easy_setopt(HTTP_HANDLE, CURLOPT_READDATA, this);
    curl_easy_setopt(HTTP_HANDLE, CURLOPT_SEEKDATA, this);
    curl_easy_setopt(HTTP_HANDLE, CURLOPT_WRITEHEADER, this);
    curl_easy_setopt(HTTP_HANDLE, CURLOPT_WRITEDATA, this);

    JS::Value tmp = JS_GetReservedSlot(req, 0);
    if(!tmp.isString()) {
        JS_ReportErrorUTF8(cx, "Referer must be a string.");
        return false;
    }

    std::string referer = js_to_string(cx, JS::Rooted<JS::Value>(cx, tmp));
    curl_easy_setopt(HTTP_HANDLE, CURLOPT_REFERER, referer.c_str());

    curl_easy_setopt(HTTP_HANDLE, CURLOPT_CUSTOMREQUEST, _method.c_str());
    curl_easy_setopt(HTTP_HANDLE, CURLOPT_URL, _url.c_str());
    curl_easy_setopt(HTTP_HANDLE, CURLOPT_HTTPHEADER, _req_headers);

    curl_easy_setopt(HTTP_HANDLE, CURLOPT_NOBODY, 0);
    curl_easy_setopt(HTTP_HANDLE, CURLOPT_FOLLOWLOCATION, 1);
    curl_easy_setopt(HTTP_HANDLE, CURLOPT_UPLOAD, 0);

    if(_method == "HEAD") {
        curl_easy_setopt(HTTP_HANDLE, CURLOPT_NOBODY, 1);
        curl_easy_setopt(HTTP_HANDLE, CURLOPT_FOLLOWLOCATION, 0);
    } else if(_method == "POST" || _method == "PUT") {
        curl_easy_setopt(HTTP_HANDLE, CURLOPT_UPLOAD, 1);
        curl_easy_setopt(HTTP_HANDLE, CURLOPT_FOLLOWLOCATION, 0);
    }

    if(body.size()) {
        curl_easy_setopt(HTTP_HANDLE, CURLOPT_INFILESIZE, body.size());
    } else {
        curl_easy_setopt(HTTP_HANDLE, CURLOPT_INFILESIZE, 0);
    }

    // curl_easy_setopt(HTTP_HANDLE, CURLOPT_VERBOSE, 1);

    CURLcode err = curl_easy_perform(HTTP_HANDLE);
    if(err != CURLE_OK) {
        const char* fmt = "Failed to execute HTTP request: %d :: %s";
        JS_ReportErrorUTF8(cx, fmt, err, ERRBUF);
        return false;
    }

    if(!_resp_headers) {
        JS_ReportErrorUTF8(cx, "Failed to recieve HTTP headers.");
        return false;
    }

    tmp = JS::ObjectValue(*_resp_headers);
    JS::RootedValue rtmp1(cx, tmp);
    if(!JS_DefineProperty(cx, robj, "_headers", rtmp1, JSPROP_READONLY)) {
        JS_ReportErrorUTF8(cx, "INTERNAL: Failed to set response headers.");
        return false;
    }

    if(_recvbuf.str().size()) {
        JSString* jsbody = string_to_js(cx, _recvbuf.str());
        if(!jsbody) {
            // If we can't decode the body as UTF-8 we forcefully
            // convert it to a string by just forcing each byte
            // to a char16_t.
            std::string respbody = _recvbuf.str();
            jsbody = JS_NewStringCopyN(cx, respbody.c_str(), respbody.size());
            if(!jsbody) {
                if(!JS_IsExceptionPending(cx)) {
                    JS_ReportErrorUTF8(cx, "INTERNAL: Failed to decode body.");
                }
                return false;
            }
        }
        tmp = JS::StringValue(jsbody);
    } else {
        tmp = JS_GetEmptyStringValue(cx);
    }

    JS::RootedValue rtmp2(cx, tmp);
    if(!JS_DefineProperty(cx, robj, "responseText", rtmp2, JSPROP_READONLY)) {
        JS_ReportErrorUTF8(cx, "INTERNAL: Failed to set responseText.");
        return false;
    }

    return true;
}


int
HTTP::get_last_status()
{
    return _last_status;
}


size_t
HTTP::send_body(void* data, size_t size, size_t nmem)
{
    size_t length = size * nmem;
    size_t towrite = _body.size() - _sent;

    // Assume this is cURL trying to resend a request that
    // failed.
    if(towrite == 0 && _sent_once == 1) {
        _sent = 0;
        _sent_once = 0;
        towrite = _body.size();
    } else if(towrite == 0) {
        _sent_once = 1;
    }

    if(length < towrite) {
        towrite = length;
    }

    memcpy(data, _body.c_str() + _sent, towrite);
    _sent += towrite;

    return towrite;
}


int
HTTP::seek_body(curl_off_t offset, int origin)
{
    if(origin != SEEK_SET) {
        return CURL_SEEKFUNC_FAIL;
    }

    _sent = (size_t) offset;

    return CURL_SEEKFUNC_OK;
}


size_t
HTTP::recv_header(void* ptr, size_t size, size_t nmem)
{
    char* header = static_cast<char*>(ptr);
    size_t length = size * nmem;

    if(length > 7 && strncasecmp(header, "HTTP/1.", 7) == 0) {
        if(length < 12) {
            return CURLE_WRITE_ERROR;
        }

        std::string code(header + 9, 3);
        _last_status = atoi(code.c_str());

        _resp_headers = JS_NewArrayObject(_cx, 0);
        if(!_resp_headers) {
            return CURLE_WRITE_ERROR;
        }

        return length;
    }

    // We get a notice at the \r\n\r\n after headers.
    if(length <= 2) {
        return length;
    }

    // Append the new header to our array.
    JSString* jsheader = string_to_js(_cx, std::string(header, length));
    if(!jsheader) {
        return CURLE_WRITE_ERROR;
    }

    JS::RootedObject obj(_cx, _resp_headers);
    uint32_t num_headers;
    if(!JS_GetArrayLength(_cx, obj, &num_headers)) {
        return CURLE_WRITE_ERROR;
    }

    JS::RootedString rjsheader(_cx, jsheader);
    if(!JS_SetElement(_cx, obj, num_headers, rjsheader)) {
        return CURLE_WRITE_ERROR;
    }

    return length;
}


size_t
HTTP::recv_body(void* ptr, size_t size, size_t nmem)
{
    char* data = static_cast<char*>(ptr);
    size_t length = size * nmem;
    _recvbuf << std::string(data, length);
    return length;
}


bool
http_ctor(JSContext* cx, JSObject* req)
{
    HTTP* http = new HTTP();

    if(!http) {
        JS_ReportErrorUTF8(cx, "Failed to create CouchHTTP instance.");
        return false;
    }

    JS_SetPrivate(req, http);

    return true;
}


void
http_dtor(JSFreeOp* fop, JSObject* obj)
{
    HTTP* http = static_cast<HTTP*>(JS_GetPrivate(obj));
    if(http) {
        delete http;
    }
}


bool
http_open(JSContext* cx, JSObject* req, JS::Value mth, JS::Value url, JS::Value snc)
{
    HTTP* http = static_cast<HTTP*>(JS_GetPrivate(req));

    if(!http) {
        JS_ReportErrorUTF8(cx, "Invalid CouchHTTP instance.");
        return false;
    }

    if(!mth.isString()) {
        JS_ReportErrorUTF8(cx, "Method bust be a string.");
        return false;
    }

    if(!url.isString()) {
        JS_ReportErrorUTF8(cx, "The URL must be a string.");
        return false;
    }

    if(snc.isBoolean() && snc.isTrue()) {
        JS_ReportErrorUTF8(cx, "Synchronous flag must be false.");
        return false;
    }

    if(!http->set_method(js_to_string(cx, JS::Rooted<JS::Value>(cx, mth)))) {
        JS_ReportErrorUTF8(cx, "Invalid method specified.");
        return false;
    }

    if(!http->set_url(js_to_string(cx, JS::Rooted<JS::Value>(cx, url)))) {
        JS_ReportErrorUTF8(cx, "Invalid URL specified.");
    }

    http->clear_headers();

    // Disable Expect: 100-continue
    return http->add_header("Expect", "");
}


bool
http_set_hdr(JSContext* cx, JSObject* req, JS::Value name, JS::Value value)
{
    HTTP* http = static_cast<HTTP*>(JS_GetPrivate(req));

    if(!http) {
        JS_ReportErrorUTF8(cx, "Invalid CouchHTTP instance.");
        return false;
    }

    if(!name.isString()) {
        JS_ReportErrorUTF8(cx, "Header names must be strings.");
        return false;
    }

    if(!value.isString()) {
        JS_ReportErrorUTF8(cx, "Header values must be strings.");
        return false;
    }

    std::string namestr = js_to_string(cx, JS::Rooted<JS::Value>(cx, name));
    std::string valuestr = js_to_string(cx, JS::Rooted<JS::Value>(cx, value));

    return http->add_header(namestr, valuestr);
}


bool
http_send(JSContext* cx, JSObject* req, JS::Value body)
{
    HTTP* http = static_cast<HTTP*>(JS_GetPrivate(req));

    if(!http) {
        JS_ReportErrorUTF8(cx, "Invalid CouchHTTP instance.");
        return false;
    }

    std::string bodystr;
    if(!body.isUndefined()) {
        if(!body.isString()) {
            JS_ReportErrorUTF8(cx, "Body must be a string.");
            return false;
        }
        bodystr = js_to_string(cx, JS::Rooted<JS::Value>(cx, body));
    }

    return http->send(cx, req, bodystr);
}


int
http_status(JSContext* cx, JSObject* req)
{
    HTTP* http = static_cast<HTTP*>(JS_GetPrivate(req));

    if(!http) {
        JS_ReportErrorUTF8(cx, "Invalid CouchHTTP instance.");
        return false;
    }

    return http->get_last_status();
}


bool
http_uri(JSContext* cx, JSObject* req, couch_args* args, JS::Value* uri_val)
{
    // Default is http://localhost:15986/ when no uri file is specified
    if(!args->uri_file) {
        JSString* uri_str = JS_NewStringCopyZ(cx, "http://localhost:15986/");
        *uri_val = JS::StringValue(uri_str);
        JS_SetReservedSlot(req, 0, *uri_val);
        return true;
    }

    // Else check to see if the base url is cached in a reserved slot
    *uri_val = JS_GetReservedSlot(req, 0);
    if(!(*uri_val).isUndefined()) {
        return true;
    }

    // Read the first line of the couch.uri file.
    FILE* uri_fp = fopen(args->uri_file, "r");
    if(!uri_fp) {
        JS_ReportErrorUTF8(cx, "Faield to open couch.uri file.");
        return false;
    }

    JSString* uri_str = couch_readline(cx, uri_fp);
    fclose(uri_fp);

    if(!uri_str) {
        JS_ReportErrorUTF8(cx, "Failed to read couch.uri file.");
        return false;
    }

    *uri_val = JS::StringValue(uri_str);
    JS_SetReservedSlot(req, 0, *uri_val);
    return true;
}

#endif /* HAVE_CURL */
