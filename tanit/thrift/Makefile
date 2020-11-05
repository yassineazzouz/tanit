gen:
	thrift -gen py:package_prefix=tanit.thrift. -out ./ protocol/model.thrift
	thrift -gen py:package_prefix=tanit.thrift. -out ./ protocol/dfs.thrift
	thrift -gen py:package_prefix=tanit.thrift. -out ./ protocol/master.thrift
	thrift -gen py:package_prefix=tanit.thrift. -out ./ protocol/worker.thrift
	thrift -gen py:package_prefix=tanit.thrift. -out ./ protocol/filesystem.thrift

clean:
	rm -rf tanit/thrift/master
	rm -rf tanit/thrift/worker
	rm -rf tanit/thrift/common