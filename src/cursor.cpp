
// This file is part of node-lmdb, the Node.js binding for lmdb
// Copyright (c) 2013 Timur Kristóf
// Licensed to you under the terms of the MIT license
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#include "node-lmdb.h"

using namespace v8;
using namespace node;

CursorWrap::CursorWrap(MDB_cursor *cursor) {
    this->cursor = cursor;
}

CursorWrap::~CursorWrap() {
    if (this->cursor) {
        mdb_cursor_close(this->cursor);
    }
}

Handle<Value> CursorWrap::ctor(const Arguments &args) {
    // Get arguments
    TxnWrap *tw = ObjectWrap::Unwrap<TxnWrap>(args[0]->ToObject());
    DbiWrap *dw = ObjectWrap::Unwrap<DbiWrap>(args[1]->ToObject());

    // Open the cursor
    MDB_cursor *cursor;
    int rc = mdb_cursor_open(tw->txn, dw->dbi, &cursor);
    if (rc != 0) {
        ThrowException(Exception::Error(String::New(mdb_strerror(rc))));
        return Undefined();
    }
    
    // Create wrapper
    CursorWrap* cw = new CursorWrap(cursor);
    cw->Wrap(args.This());

    return args.This();
}

Handle<Value> CursorWrap::close(const Arguments &args) {
    CursorWrap *cw = ObjectWrap::Unwrap<CursorWrap>(args.This());
    mdb_cursor_close(cw->cursor);
    cw->cursor = NULL;
    return Undefined();
}

void CursorWrap::setupExports(Handle<Object> exports) {
    // CursorWrap: Prepare constructor template
    Local<FunctionTemplate> cursorTpl = FunctionTemplate::New(CursorWrap::ctor);
    cursorTpl->SetClassName(String::NewSymbol("Cursor"));
    cursorTpl->InstanceTemplate()->SetInternalFieldCount(1);
    // CursorWrap: Add functions to the prototype
    cursorTpl->PrototypeTemplate()->Set(String::NewSymbol("close"), FunctionTemplate::New(CursorWrap::close)->GetFunction());
    
    // CursorWrap: Get constructor
    Persistent<Function> cursorCtor = Persistent<Function>::New(cursorTpl->GetFunction());
    
    // Set exports
    exports->Set(String::NewSymbol("Cursor"), cursorCtor);
}



