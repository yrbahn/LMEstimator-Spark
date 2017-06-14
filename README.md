# Spark LM

## 설정하기
	sparklm{
		// 입력
		inputs = ["hdfs://.../*.txt"]
		
		// 결과 
		output_dir  = "hdfs://...."
		output_foramt = "arpa" // ex) text, apra

		//사전 설정
		dictionary {
			//사전 로딩 여부
			is_provided = false
			start_symbol = "<s>"
			end_symbol = "</s>"
			unk = "<UNK>"
			word_count = 1000000
			save_path = ""
		}

		//ngram 설정
		ngram {
			order = 3
			discount = 0.5
		}
	
		//토근자 설정 (ex) twitter
		tokenizer = "twitter"
	}

## 학습방법
#### Step1. 사전생성 하기
	sparklm-assembly-0.1.jar --command create_dict --config <conf_file>

#### Step2. NGram 빈도수 구하기 (Interpolated Knersey-Ney Smoothing를 구할때는 이 단계 생략
	sparklm-assembly-0.1.jar --command count_ngrams --config <conf_file>

#### Step3. NGram adjusted 빈도수 구하기 (interpolated Knersey-Ney Smoothing)
	sparklm-assembly-0.1.jar --command adjusted_count --config <conf_file>

#### Step4. Discount 값 생성하기 (NGram 빈도 수나 수정된 빈도수를 통해 Discount 맵 생성
	sparklm-assembly-0.1.jar --command gen_discount --config <conf_file>

#### Step5. NGram Uninterpolated 확률값 구하기
	sparklm-assembly-0.1.jar --command uninterp_prob --config <conf_file>

#### Step6. NGram Interpolated 확률값 구하기
	sparklm-assembly-0.1.jar --command interp_prob --config <conf_file>

#### Step7. BackOff 값 생성하기
	sparklm-assembly-0.1.jar --command backoff --config <conf_file>



## 모델 생성및 client프로그램 시동하기
scripts/lm_query.py \<arpa\>

	python query_lm.py arpa/test.arpa
	Loading the LM will be faster if you build a binary file.
	Reading /Users/roy/work/sparklm/scripts/arpa/test.arpa
	----5---10---15---20---25---30---35---40---45---50---55---60---65---70---75---80---85---90---95--100
	The ARPA file is missing <unk>.  Substituting log10 probability -100.
	****************************************************************************************************
	sentence:조사가 끝난뒤에
	score: -6.91878557205
	sentence:조사가 끝난뒤
	score: -5.41363477707
	sentence:조사가 없어요
	score: -204.681259155

## gRPC 
	### gRPC 서버
	grpc/lm_server.py <arpa>
	
	### gRPC 클라이언트
	qrpc/lm_client.py 

## REFERENCES

Scalable Modified Kneser-Ney Language Model Estimation - http://www.aclweb.org/anthology/P13-2121.pdf

