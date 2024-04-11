// Code generated by protoc-gen-go-async. DO NOT EDIT.
// versions:
// - protoc-gen-go-async v0.0.1
// - protoc             v4.22.0
// source: test.proto

package main

import (
	context "context"
	async "github.com/lwbio/async/async"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the kratos package it is being compiled against.
var _ = new(context.Context)
var _ = new(async.Server)

const AsyncOperationServiceAdd = "/async.Service/Add"
const AsyncOperationServiceSub = "/async.Service/Sub"

type ServiceAsyncServer interface {
	Add(context.Context, *AddRequest) (*AddResponse, error)
	Sub(context.Context, *SubRequest) (*SubResponse, error)
}

func RegisterServiceAsyncServer(s *async.Server, srv ServiceAsyncServer) {
	s.Register(AsyncOperationServiceAdd, _Service_Add0_Async_Handler(srv))
	s.Register(AsyncOperationServiceSub, _Service_Sub0_Async_Handler(srv))
}

func _Service_Add0_Async_Handler(srv ServiceAsyncServer) func(ctx async.Context) error {
	return func(ctx async.Context) error {
		var in AddRequest
		if err := ctx.Decode(&in); err != nil {
			return err
		}
		out, err := srv.Add(ctx, &in)
		if err != nil {
			return err
		}
		return ctx.Encode(out)
	}
}

func _Service_Sub0_Async_Handler(srv ServiceAsyncServer) func(ctx async.Context) error {
	return func(ctx async.Context) error {
		var in SubRequest
		if err := ctx.Decode(&in); err != nil {
			return err
		}
		out, err := srv.Sub(ctx, &in)
		if err != nil {
			return err
		}
		return ctx.Encode(out)
	}
}
