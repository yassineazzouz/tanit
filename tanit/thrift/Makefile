gen:
	thrift -gen py -out ./ protocol/dfs.thrift
	thrift -gen py -out ./ protocol/master.thrift
	thrift -gen py -out ./ protocol/worker.thrift
	thrift -gen py -out ./ protocol/filesystem.thrift

clean:
	rm -rf ./master
	rm -rf ./worker