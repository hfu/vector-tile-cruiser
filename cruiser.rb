%w{rubygems find thread time json}.each{|s| require s}

N_THREADS = 8
Q_SIZE = 2000
WAIT = 10
CONTINUE = nil

$threads = Array.new(N_THREADS)
$status = {:ok => 0, :ng => 0, :path => nil}
$keys = %w{ok ng path}
$q = SizedQueue.new(Q_SIZE)

$threads.size.times {|i|
  $threads[i] = Thread.new(i) do
    while o = $q.pop
      begin
        JSON::parse(File.read(o[:path]))
        $status[:ok] += 1
      rescue
        print "error while processing #{o[:path]}!\n"
        print "#{$!}\n"
        $status[:ng] += 1
      end
    end
  end
}

watcher = Thread.new do
  while $threads.reduce(false) {|any_alive, t| any_alive or t.alive?}
    last_status = $status.clone
    sleep WAIT
    print "#{Time.now.iso8601[11..18]} #{$status[:path]} #{$q.size} #{$status}\n"
  end
end

$count = 0
Find.find('18') {|path|
  next unless path.end_with?('geojson')
  $count += 1
  $status[:path] = path
  $q.push({:path => path})
}

$threads.size.times {|i| $q.push(nil)}
$threads.each {|t| t.join}
watcher.join
