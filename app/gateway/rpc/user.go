package rpc

import (
	"context"

	"github.com/CocaineCong/micro-todoList/idl"
)

// UserLogin 用户登陆
func UserLogin(ctx context.Context, req *idl.UserRequest) (resp *idl.UserDetailResponse, err error) {
	resp, err = UserService.UserLogin(ctx, req)
	if err != nil {
		return
	}

	return
}

// UserRegister 用户注册
func UserRegister(ctx context.Context, req *idl.UserRequest) (resp *idl.UserDetailResponse, err error) {
	resp, err = UserService.UserRegister(ctx, req)
	if err != nil {
		return
	}

	return
}