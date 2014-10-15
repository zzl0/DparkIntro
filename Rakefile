Rake.application.options.trace_rules = true

SOURCE_FILES = Rake::FileList.new("**/*.dot")

task :default => :png
task :png => SOURCE_FILES.ext(".png")

def source_for_png(name)
    SOURCE_FILES.detect {|f| f.ext('') == name.ext('')}
end

rule ".png" => ->(f){source_for_png(f)} do |t|
    sh "dot -Tpng -o #{t.name} #{t.source}"
end
