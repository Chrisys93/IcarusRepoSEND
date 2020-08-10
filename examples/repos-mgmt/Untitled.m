clear

H1 = dlmread('hybrid.txt', ',', 0, 0);
HR1 = dlmread('hybrid_repo.txt', ',', 0, 1);
HRP1 = dlmread('hybrid_pro_repo.txt', ',', 0, 1);
HRR1 = dlmread('hybrid_re_repo.txt', ',', 0, 1);
HRS1 = dlmread('hybrid_spec_repo.txt', ',', 0, 1);

% RG1 = dlmread('gen_r_replicas.txt', ',', 0, 0);
% RP1 = dlmread('pro_r_replicas.txt', ',', 0, 1);
% RR1 = dlmread('re_r_replicas.txt', ',', 0, 1);
% RS1 = dlmread('spec_r_replicas.txt', ',', 0, 1);
% 
% SG1 = dlmread('gen_s_replicas.txt', ',', 0, 0);
% SP1 = dlmread('pro_s_replicas.txt', ',', 0, 1);
% SR1 = dlmread('re_s_replicas.txt', ',', 0, 1);
% SS1 = dlmread('spec_s_replicas.txt', ',', 0, 1);

OH1 = dlmread('hybrid_overheads.txt', ',', 0, 0);
OG1 = dlmread('gen_overheads.txt', ',', 0, 0);
OP1 = dlmread('pro_overheads.txt', ',', 0, 1);
OR1 = dlmread('re_overheads.txt', ',', 0, 1);
OS1 = dlmread('spec_overheads.txt', ',', 0, 1);



AH1 = dlmread('Azure results/hybrid.txt', ',', 0, 0);
AHR1 = dlmread('Azure results/hybrid_repo.txt', ',', 0, 1);
AHRP1 = dlmread('Azure results/hybrid_pro_repo.txt', ',', 0, 1);
AHRR1 = dlmread('Azure results/hybrid_re_repo.txt', ',', 0, 1);
AHRS1 = dlmread('Azure results/hybrid_spec_repo.txt', ',', 0, 1);

% RG1 = dlmread('Azure results/gen_r_replicas.txt', ',', 0, 0);
% RP1 = dlmread('Azure results/pro_r_replicas.txt', ',', 0, 1);
% RR1 = dlmread('Azure results/re_r_replicas.txt', ',', 0, 1);
% RS1 = dlmread('Azure results/spec_r_replicas.txt', ',', 0, 1);
% 
% SG1 = dlmread('Azure results/gen_s_replicas.txt', ',', 0, 0);
% SP1 = dlmread('Azure results/pro_s_replicas.txt', ',', 0, 1);
% SR1 = dlmread('Azure results/re_s_replicas.txt', ',', 0, 1);
% SS1 = dlmread('Azure results/spec_s_replicas.txt', ',', 0, 1);

OAH1 = dlmread('Azure results/hybrid_overheads.txt', ',', 0, 0);
OAG1 = dlmread('Azure results/gen_overheads.txt', ',', 0, 0);
OAP1 = dlmread('Azure results/pro_overheads.txt', ',', 0, 1);
OAR1 = dlmread('Azure results/re_overheads.txt', ',', 0, 1);
OAS1 = dlmread('Azure results/spec_overheads.txt', ',', 0, 1);



GH1 = dlmread('Google results/hybrid.txt', ',', 0, 0);
GHR1 = dlmread('Google results/hybrid_repo.txt', ',', 0, 1);
GHRP1 = dlmread('Google results/hybrid_pro_repo.txt', ',', 0, 1);
GHRR1 = dlmread('Google results/hybrid_re_repo.txt', ',', 0, 1);
GHRS1 = dlmread('Google results/hybrid_spec_repo.txt', ',', 0, 1);

% RG1 = dlmread('Google results/gen_r_replicas.txt', ',', 0, 0);
% RP1 = dlmread('Google results/pro_r_replicas.txt', ',', 0, 1);
% RR1 = dlmread('Google results/re_r_replicas.txt', ',', 0, 1);
% RS1 = dlmread('Google results/spec_r_replicas.txt', ',', 0, 1);
% 
% SG1 = dlmread('Google results/gen_s_replicas.txt', ',', 0, 0);
% SP1 = dlmread('Google results/pro_s_replicas.txt', ',', 0, 1);
% SR1 = dlmread('Google results/re_s_replicas.txt', ',', 0, 1);
% SS1 = dlmread('Google results/spec_s_replicas.txt', ',', 0, 1);

OGH1 = dlmread('Google results/hybrid_overheads.txt', ',', 0, 0);
OGG1 = dlmread('Google results/gen_overheads.txt', ',', 0, 0);
OGP1 = dlmread('Google results/pro_overheads.txt', ',', 0, 1);
OGR1 = dlmread('Google results/re_overheads.txt', ',', 0, 1);
OGS1 = dlmread('Google results/spec_overheads.txt', ',', 0, 1);

% TODO: Should also include LABEL DISTRIBUTIONS! 
% (but later, once we get partial results)

[m, n] = size(H1);

for i = 1:1000
H1_perf(i) = sum(H1(i+200,:))/n;
end

[p, q] = size(HR1);

for i = 1:1000
HR1_perf(i) = sum(HR1(i+200, :))/q;
end

[r, s] = size(HRP1);

for i = 1:1000
HRP1_perf(i) = sum(HRP1(i+200,:))/s;
end

[t, u] = size(HRR1);

for i = 1:1000
HRR1_perf(i) = sum(HRR1(i+200, :))/u;
end

[v, w] = size(HRS1);

for i = 1:1000
HRS1_perf(i) = sum(HRS1(i+200, :))/w;
end




[m, n] = size(OH1);

for i = 1:1000
OH1_perf(i) = mean(OH1(i+200,:))/5e5;
end

[p, q] = size(OG1);

for i = 1:1000
OG1_perf(i) = mean(OG1(i+200,:))/5e5;
end

[r, s] = size(HRP1);

for i = 1:1000
OP1_perf(i) = mean(OP1(i+200,:))/5e5;
end

[t, u] = size(HRR1);

for i = 1:1000
OR1_perf(i) = mean(OR1(i+200, :))/5e5;
end

[v, w] = size(HRS1);

for i = 1:1000
OS1_perf(i) = mean(OS1(i+200, :))/5e5;
end



fitted_H1 = polyfit(1:1000, H1_perf, 1);
fitted_HR1 = polyfit(1:1000, HR1_perf, 1);
fitted_HRP1 = polyfit(1:1000, HRP1_perf, 1);
fitted_HRR1 = polyfit(1:1000, HRR1_perf, 1);
fitted_HRS1 = polyfit(1:1000, HRS1_perf, 1);


for i = 1:5

    H_max(i) = max(H1_perf(((i-1)*200+1):(i*200)));
    H_avg(i) = mean(H1_perf(((i-1)*200+1):(i*200)));
    H_min(i) = min(H1_perf(((i-1)*200+1):(i*200)));

end


for i = 1:5

    HR_max(i) = max(HR1_perf(((i-1)*200+1):(i*200)));
    HR_avg(i) = mean(HR1_perf(((i-1)*200+1):(i*200)));
    HR_min(i) = min(HR1_perf(((i-1)*200+1):(i*200)));

end


for i = 1:5

    HRP_max(i) = max(HRP1_perf(((i-1)*200+1):(i*200)));
    HRP_avg(i) = mean(HRP1_perf(((i-1)*200+1):(i*200)));
    HRP_min(i) = min(HRP1_perf(((i-1)*200+1):(i*200)));

end


for i = 1:5

    HRR_max(i) = max(HRR1_perf(((i-1)*200+1):(i*200)));
    HRR_avg(i) = mean(HRR1_perf(((i-1)*200+1):(i*200)));
    HRR_min(i) = min(HRR1_perf(((i-1)*200+1):(i*200)));

end


for i = 1:5

    HRS_max(i) = max(HRS1_perf(((i-1)*200+1):(i*200)));
    HRS_avg(i) = mean(HRS1_perf(((i-1)*200+1):(i*200)));
    HRS_min(i) = min(HRS1_perf(((i-1)*200+1):(i*200)));

end

% TODO: AVERAGES AND STD DEV.
% Plus other datasets
% BAR CHARTS!




% TODO: Should also include LABEL DISTRIBUTIONS! 
% (but later, once we get partial results)

[m, n] = size(GH1);

for i = 1:1100
GH1_perf(i) = sum(GH1(i+200,:))/n;
end

[p, q] = size(GHR1);

for i = 1:1100
GHR1_perf(i) = sum(GHR1(i+200, :))/q;
end

[r, s] = size(GHRP1);

for i = 1:1100
GHRP1_perf(i) = sum(GHRP1(i+200,:))/s;
end

[t, u] = size(GHRR1);

for i = 1:1100
GHRR1_perf(i) = sum(GHRR1(i+200, :))/u;
end

[v, w] = size(GHRS1);

for i = 1:1100
GHRS1_perf(i) = sum(GHRS1(i+200, :))/w;
end




[m, n] = size(OGH1);

for i = 1:1100
OGH1_perf(i) = mean(OGH1(i+200,:))/5e5;
end

[p, q] = size(OG1);

for i = 1:1100
OGG1_perf(i) = mean(OGG1(i+200+100,:))/5e5;
end

[r, s] = size(GHRP1);

for i = 1:1100
OGP1_perf(i) = mean(OGP1(i+200,:))/5e5;
end

[t, u] = size(GHRR1);

for i = 1:1100
OGR1_perf(i) = mean(OGR1(i+200, :))/5e5;
end

[v, w] = size(GHRS1);

for i = 1:1100
OGS1_perf(i) = mean(OGS1(i+200, :))/5e5;
end



fitted_GH1 = polyfit(1:1100, GH1_perf, 1);
fitted_GHR1 = polyfit(1:1100, GHR1_perf, 1);
fitted_GHRP1 = polyfit(1:1100, GHRP1_perf, 1);
fitted_GHRR1 = polyfit(1:1100, GHRR1_perf, 1);
fitted_GHRS1 = polyfit(1:1100, GHRS1_perf, 1);


for i = 1:5

    GH_max(i) = max(GH1_perf(((i-1)*200+101):(i*200+100)));
    GH_avg(i) = mean(GH1_perf(((i-1)*200+101):(i*200+100)));
    GH_min(i) = min(GH1_perf(((i-1)*200+101):(i*200+100)));

end


for i = 1:5

    GHR_max(i) = max(GHR1_perf(((i-1)*200+101):(i*200+100)));
    GHR_avg(i) = mean(GHR1_perf(((i-1)*200+101):(i*200+100)));
    GHR_min(i) = min(GHR1_perf(((i-1)*200+101):(i*200+100)));

end


for i = 1:5

    GHRP_max(i) = max(GHRP1_perf(((i-1)*200+101):(i*200+100)));
    GHRP_avg(i) = mean(GHRP1_perf(((i-1)*200+101):(i*200+100)));
    GHRP_min(i) = min(GHRP1_perf(((i-1)*200+101):(i*200+100)));

end


for i = 1:5

    GHRR_max(i) = max(GHRR1_perf(((i-1)*200+101):(i*200+100)));
    GHRR_avg(i) = mean(GHRR1_perf(((i-1)*200+101):(i*200+100)));
    GHRR_min(i) = min(GHRR1_perf(((i-1)*200+101):(i*200+100)));

end


for i = 1:5

    GHRS_max(i) = max(GHRS1_perf(((i-1)*200+101):(i*200+100)));
    GHRS_avg(i) = mean(GHRS1_perf(((i-1)*200+101):(i*200+100)));
    GHRS_min(i) = min(GHRS1_perf(((i-1)*200+101):(i*200+100)));

end



% TODO: Should also include LABEL DISTRIBUTIONS! 
% (but later, once we get partial results)

[m, n] = size(AH1);

for i = 1:1000
AH1_perf(i) = sum(AH1(i+200,:))/n;
end

[p, q] = size(AHR1);

for i = 1:1000
AHR1_perf(i) = sum(AHR1(i+200, :))/q;
end

[r, s] = size(AHRP1);

for i = 1:1000
AHRP1_perf(i) = sum(AHRP1(i+200,:))/s;
end

[t, u] = size(AHRR1);

for i = 1:1000
AHRR1_perf(i) = sum(AHRR1(i+200, :))/u;
end

[v, w] = size(AHRS1);

for i = 1:1000
AHRS1_perf(i) = sum(AHRS1(i+200, :))/w;
end




[m, n] = size(OAH1);

for i = 1:1000
OAH1_perf(i) = mean(OAH1(i+200,:))/5e5;
end

[p, q] = size(OAG1);

for i = 1:1000
OAG1_perf(i) = mean(OAG1(i+200,:))/5e5;
end

[r, s] = size(AHRP1);

for i = 1:1000
OAP1_perf(i) = mean(OAP1(i+200,:))/5e5;
end

[t, u] = size(AHRR1);

for i = 1:1000
OAR1_perf(i) = mean(OAR1(i+200, :))/5e5;
end

[v, w] = size(AHRS1);

for i = 1:1000
OAS1_perf(i) = mean(OAS1(i+200, :))/5e5;
end



fitted_AH1 = polyfit(1:1000, AH1_perf, 1);
fitted_AHR1 = polyfit(1:1000, AHR1_perf, 1);
fitted_AHRP1 = polyfit(1:1000, AHRP1_perf, 1);
fitted_AHRR1 = polyfit(1:1000, AHRR1_perf, 1);
fitted_AHRS1 = polyfit(1:1000, AHRS1_perf, 1);


for i = 1:5

    AH_max(i) = max(AH1_perf(((i-1)*200+1):(i*200)));
    AH_avg(i) = mean(AH1_perf(((i-1)*200+1):(i*200)));
    AH_min(i) = min(AH1_perf(((i-1)*200+1):(i*200)));

end


for i = 1:5

    AHR_max(i) = max(AHR1_perf(((i-1)*200+1):(i*200)));
    AHR_avg(i) = mean(AHR1_perf(((i-1)*200+1):(i*200)));
    AHR_min(i) = min(AHR1_perf(((i-1)*200+1):(i*200)));

end


for i = 1:5

    AHRP_max(i) = max(AHRP1_perf(((i-1)*200+1):(i*200)));
    AHRP_avg(i) = mean(AHRP1_perf(((i-1)*200+1):(i*200)));
    AHRP_min(i) = min(AHRP1_perf(((i-1)*200+1):(i*200)));

end


for i = 1:5

    AHRR_max(i) = max(AHRR1_perf(((i-1)*200+1):(i*200)));
    AHRR_avg(i) = mean(AHRR1_perf(((i-1)*200+1):(i*200)));
    AHRR_min(i) = min(AHRR1_perf(((i-1)*200+1):(i*200)));

end


for i = 1:5

    AHRS_max(i) = max(AHRS1_perf(((i-1)*200+1):(i*200)));
    AHRS_avg(i) = mean(AHRS1_perf(((i-1)*200+1):(i*200)));
    AHRS_min(i) = min(AHRS1_perf(((i-1)*200+1):(i*200)));

end

% TODO: AVERAGES AND STD DEV.
% Plus other datasets
% BAR CHARTS!





figure

plot(1:2:1000, H1_perf(2:2:1000), 2:2:1000, HR1_perf(2:2:1000), 2:2:1000, HRP1_perf(2:2:1000), 2:2:1000, HRR1_perf(2:2:1000), 2:2:1000, HRS1_perf(2:2:1000))
xlabel('Number of requests (1000)')
ylabel('Request Satisfaction Rate (%)')
%legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid')


hold on

plot(2:2:1000, polyval(fitted_H1, 2:2:1000), 2:2:1000, polyval(fitted_HR1, 2:2:1000), 2:2:1000, polyval(fitted_HRP1, 2:2:1000), 2:2:1000, polyval(fitted_HRR1, 2:2:1000), 2:2:1000, polyval(fitted_HRS1, 2:2:1000), 'LineWidth', 2)
xlabel('Number of requests (1000)')
ylabel('Request Satisfaction Rate (%)')
legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid')


% 
% figure
% hold on
% 
% errorbar(1:200:1000, H_avg, H_min, H_max)
% errorbar(1:200:1000, HR_avg, HR_min, HR_max)
% errorbar(1:200:1000, HRP_avg, HRP_min, HRP_max)
% errorbar(1:200:1000, HRR_avg, HRR_min, HRR_max)
% errorbar(1:200:1000, HRS_avg, HRS_min, HRS_max)
% xlabel('Number of requests (500)')
% ylabel('Request Satisfaction Rate (%)')
% legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid')


figure
hold on
bar([1:200:1000], [H_avg', HR_avg', HRP_avg', HRR_avg', HRS_avg'], 'group')
set(gca,'xtick',200:200:1200,'XTickLabel', 200:200:1200)
% plot(1:200:1000, H_min, 1:200:1000, HR_min, 1:200:1000, HRP_min, 1:200:1000, HRR_min, 1:200:1000, HRS_min)
% plot(1:200:1000, H_max, 1:200:1000, HR_max, 1:200:1000, HRP_max, 1:200:1000, HRR_max, 1:200:1000, HRS_max)
xlabel('Number of requests (1000)')
ylabel('Request Satisfaction Rate (%)')
legend2 = legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid');



figure

plot(2:2:1000, HR1_perf(2:2:1000), 2:2:1000, HRP1_perf(2:2:1000), 2:2:1000, HRR1_perf(2:2:1000), 2:2:1000, HRS1_perf(2:2:1000))
xlabel('Number of requests (1000)')
ylabel('Request Satisfaction Rate (%)')
legend('SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid')


figure

semilogy(1:2:1000, OH1_perf(2:2:1000), 2:2:1000, OG1_perf(2:2:1000), 2:2:1000, OP1_perf(2:2:1000), 2:2:1000, OR1_perf(2:2:1000), 2:2:1000, OS1_perf(2:2:1000))
xlabel('Number of requests (1000)')
ylabel('Normalized Overhead')
legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid')

% figure
% 
% y = polyval(fitted_HR1, 2:2:1000); % your mean vector;
% x = 2:4:1000;
% std_dev = 1;
% HR = HR1_perf(2:2:1000);
% curve1 = HR(1:2:500);
% curve2 = HR(2:2:500);
% x2 = [x, fliplr(x)];
% inBetween = [curve1, fliplr(curve2)];
% fill(x2, inBetween, 'g');
% hold on;
% plot(2:2:1000, y, 'r', 'LineWidth', 2);

H_overall_max = max(H1_perf);
HR_overall_max = max(HR1_perf);
HRP_overall_max = max(HRP1_perf);
HRR_overall_max = max(HRR1_perf);
HRS_overall_max = max(HRS1_perf);






figure

plot(1:2:1000, AH1_perf(2:2:1000), 2:2:1000, AHR1_perf(2:2:1000), 2:2:1000, AHRP1_perf(2:2:1000), 2:2:1000, AHRR1_perf(2:2:1000), 2:2:1000, AHRS1_perf(2:2:1000))
xlabel('Number of requests (1000)')
ylabel('Request Satisfaction Rate (%)')
%legend('AHybrid', 'AHybrid SEND', 'AHybrid SEND Proactive', 'AHybrid SEND Reactive', 'AHybrid SEND Specialised')


hold on

plot(2:2:1000, polyval(fitted_AH1, 2:2:1000), 2:2:1000, polyval(fitted_AHR1, 2:2:1000), 2:2:1000, polyval(fitted_AHRP1, 2:2:1000), 2:2:1000, polyval(fitted_AHRR1, 2:2:1000), 2:2:1000, polyval(fitted_AHRS1, 2:2:1000), 'LineWidth', 2)
xlabel('Number of requests (1000)')
ylabel('Request Satisfaction Rate (%)')
legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid')






figure

plot(1:2:1000, GH1_perf(2:2:1000), 2:2:1000, GHR1_perf(2:2:1000), 2:2:1000, GHRP1_perf(2:2:1000), 2:2:1000, GHRR1_perf(2:2:1000), 2:2:1000, GHRS1_perf(2:2:1000))
xlabel('Number of requests (1000)')
ylabel('Request Satisfaction Rate (%)')
%legend('GHybrid', 'GHybrid SEND', 'GHybrid SEND Proactive', 'GHybrid SEND Reactive', 'GHybrid SEND Specialised')


hold on

plot(2:2:1000, polyval(fitted_GH1, 2:2:1000), 2:2:1000, polyval(fitted_GHR1, 2:2:1000), 2:2:1000, polyval(fitted_GHRP1, 2:2:1000), 2:2:1000, polyval(fitted_GHRR1, 2:2:1000), 2:2:1000, polyval(fitted_GHRS1, 2:2:1000), 'LineWidth', 2)
xlabel('Number of requests (1000)')
ylabel('Request Satisfaction Rate (%)')
legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid')





% figure
% hold on
% 
% errorbar(1:200:1000, GH_avg, GH_min, GH_max)
% errorbar(1:200:1000, GHR_avg, GHR_min, GHR_max)
% errorbar(1:200:1000, GHRP_avg, GHRP_min, GHRP_max)
% errorbar(1:200:1000, GHRR_avg, GHRR_min, GHRR_max)
% errorbar(1:200:1000, GHRS_avg, GHRS_min, GHRS_max)
% xlabel('Number of requests (500)')
% ylabel('Request Satisfaction Rate (%)')
% legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid')


figure
hold on
bar([1:200:1000], [GH_avg', GHR_avg', GHRP_avg', GHRR_avg', GHRS_avg'], 'group')
set(gca,'xtick',200:200:1200,'XTickLabel', 200:200:1200)
legend1 = legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid');
% plot(1:200:1000, GH_min, 1:200:1000, GHR_min, 1:200:1000, GHRP_min, 1:200:1000, GHRR_min, 1:200:1000, GHRS_min)
% plot(1:200:1000, GH_max, 1:200:1000, GHR_max, 1:200:1000, GHRP_max, 1:200:1000, GHRR_max, 1:200:1000, GHRS_max)
xlabel('Number of requests (1000)')
ylabel('Request Satisfaction Rate (%)')
legend2 = legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid');





% figure
% hold on
% 
% errorbar(1:200:1000, AH_avg, AH_min, AH_max)
% errorbar(1:200:1000, AHR_avg, AHR_min, AHR_max)
% errorbar(1:200:1000, AHRP_avg, AHRP_min, AHRP_max)
% errorbar(1:200:1000, AHRR_avg, AHRR_min, AHRR_max)
% errorbar(1:200:1000, AHRS_avg, AHRS_min, AHRS_max)
% xlabel('Number of requests (500)')
% ylabel('Request Satisfaction Rate (%)')
% legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid')


figure
hold on
bar([1:200:1000], [AH_avg', AHR_avg', AHRP_avg', AHRR_avg', AHRS_avg'], 'group')
set(gca,'xtick',200:200:1200,'XTickLabel', 200:200:1200)
legend1 = legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid');
% plot(1:200:1000, AH_min, 1:200:1000, AHR_min, 1:200:1000, AHRP_min, 1:200:1000, AHRR_min, 1:200:1000, AHRS_min)
% plot(1:200:1000, AH_max, 1:200:1000, AHR_max, 1:200:1000, AHRP_max, 1:200:1000, AHRR_max, 1:200:1000, AHRS_max)
xlabel('Number of requests (1000)')
ylabel('Request Satisfaction Rate (%)')
legend2 = legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid');





% figure
% hold on
% 
% errorbar(1:200:1000, GH_avg, GH_min, GH_max)
% errorbar(1:200:1000, GHR_avg, GHR_min, GHR_max)
% errorbar(1:200:1000, GHRP_avg, GHRP_min, GHRP_max)
% errorbar(1:200:1000, GHRR_avg, GHRR_min, GHRR_max)
% errorbar(1:200:1000, GHRS_avg, GHRS_min, GHRS_max)
% xlabel('Number of requests (500)')
% ylabel('Request Satisfaction Rate (%)')
% legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid')


figure
hold on
bar([1:200:1000], [GH_avg', GHR_avg', GHRP_avg', GHRR_avg', GHRS_avg'], 'group')
set(gca,'xtick',200:200:1200,'XTickLabel', 200:200:1200)
legend1 = legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid');
% plot(1:200:1000, GH_min, 1:200:1000, GHR_min, 1:200:1000, GHRP_min, 1:200:1000, GHRR_min, 1:200:1000, GHRS_min)
% plot(1:200:1000, GH_max, 1:200:1000, GHR_max, 1:200:1000, GHRP_max, 1:200:1000, GHRR_max, 1:200:1000, GHRS_max)
xlabel('Number of requests (1000)')
ylabel('Request Satisfaction Rate (%)')
legend2 = legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid');





figure


%subplot(1, 2, 1)
h = bar([1:200:1000], [H_avg', HR_avg', HRP_avg', HRR_avg', HRS_avg'], 'group');
set(gca,'xtick',200:200:1200,'XTickLabel', 200:200:1200)
legend1 = legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid');
xlabel('Number of requests (1000)')
ylabel('Request Satisfaction Rate (%)')
legend2 = legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid');
% ax = gca();  
% % Set x tick to 1/2 way between bar groups
% ax.XTick = unique([h.XData]) + [diff(unique([h.XData]))/2, inf]; 
%subplot(1, 2, 2)
figure
g = bar([1:200:1000], [AH_avg', AHR_avg', AHRP_avg', AHRR_avg', AHRS_avg'], 'group');
set(gca,'xtick',200:200:1200,'XTickLabel', 200:200:1200)
% ax = gca();  
% % Set x tick to 1/2 way between bar groups
% ax.XTick = unique([.XData]) + [diff(unique([h.XData]))/2, inf]; 
% set(gca,'xtick',1:100:1100,'XTickLabel', 1:100:1100)
legend1 = legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid');
% plot(1:200:1000, AH_min, 1:200:1000, AHR_min, 1:200:1000, AHRP_min, 1:200:1000, AHRR_min, 1:200:1000, AHRS_min)
% plot(1:200:1000, AH_max, 1:200:1000, AHR_max, 1:200:1000, AHRP_max, 1:200:1000, AHRR_max, 1:200:1000, AHRS_max)
xlabel('Number of requests (1000)')
ylabel('Request Satisfaction Rate (%)')
legend2 = legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid');
%subplot(1, 2, 2)
figure
g = bar([1:200:1000], [GH_avg', GHR_avg', GHRP_avg', GHRR_avg', GHRS_avg'], 'group');
set(gca,'xtick',200:200:1200,'XTickLabel', 200:200:1200)
% ax = gca();  
% % Set x tick to 1/2 way between bar groups
% ax.XTick = unique([.XData]) + [diff(unique([h.XData]))/2, inf]; 
% set(gca,'xtick',1:100:1100,'XTickLabel', 1:100:1100)
legend1 = legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid');
% plot(1:200:1000, GH_min, 1:200:1000, GHR_min, 1:200:1000, GHRP_min, 1:200:1000, GHRR_min, 1:200:1000, GHRS_min)
% plot(1:200:1000, GH_max, 1:200:1000, GHR_max, 1:200:1000, GHRP_max, 1:200:1000, GHRR_max, 1:200:1000, GHRS_max)
xlabel('Number of requests (1000)')
ylabel('Request Satisfaction Rate (%)')
legend2 = legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid');





figure
hold on

subplot(1, 3, 1)
h = bar([1:200:1000], [HR_avg'./H_avg', HRP_avg'./H_avg', HRR_avg'./H_avg', HRS_avg'./H_avg'], 'group');
set(gca,'xtick',200:200:1200,'XTickLabel', 200:200:1200)
legend1 = legend('SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid');
xlabel('Number of requests (1000)')
ylabel('Performance improvement compared to Simple Hybrid Service Placement')
% ax = gca();  
% % Set x tick to 1/2 way between bar groups
% ax.XTick = unique([h.XData]) + [diff(unique([h.XData]))/2, inf]; 
subplot(1, 3, 2)
g = bar([1:200:1000], [AHR_avg'./AH_avg', AHRP_avg'./AH_avg', AHRR_avg'./AH_avg', AHRS_avg'./AH_avg'], 'group');
set(gca,'xtick',200:200:1200,'XTickLabel', 200:200:1200)
% ax = gca();  
% % Set x tick to 1/2 way between bar groups
% ax.XTick = unique([.XData]) + [diff(unique([h.XData]))/2, inf]; 
% set(gca,'xtick',1:100:1100,'XTickLabel', 1:100:1100)
legend1 = legend('SEND General-Purpose', 'Azure SEND Popularity', 'Azure SEND Function-Reactive', 'Azure SEND Hybrid');
% plot(1:200:1000, AH_min, 1:200:1000, AHR_min, 1:200:1000, AHRP_min, 1:200:1000, AHRR_min, 1:200:1000, AHRS_min)
% plot(1:200:1000, AH_max, 1:200:1000, AHR_max, 1:200:1000, AHRP_max, 1:200:1000, AHRR_max, 1:200:1000, AHRS_max)
xlabel('Number of requests (1000)')
ylabel('Performance improvement compared to Simple Hybrid Service Placement')
subplot(1, 3, 3)
g = bar([1:200:1000], [GHR_avg'./GH_avg', GHRP_avg'./GH_avg', GHRR_avg'./GH_avg', GHRS_avg'./GH_avg'], 'group');
set(gca,'xtick',200:200:1200,'XTickLabel', 200:200:1200)
% ax = gca();  
% % Set x tick to 1/2 way between bar groups
% ax.XTick = unique([.XData]) + [diff(unique([h.XData]))/2, inf]; 
% set(gca,'xtick',1:100:1100,'XTickLabel', 1:100:1100)
legend1 = legend('SEND General-Purpose', 'Azure SEND Popularity', 'Azure SEND Function-Reactive', 'Azure SEND Hybrid');
% plot(1:200:1000, GH_min, 1:200:1000, GHR_min, 1:200:1000, GHRP_min, 1:200:1000, GHRR_min, 1:200:1000, GHRS_min)
% plot(1:200:1000, GH_max, 1:200:1000, GHR_max, 1:200:1000, GHRP_max, 1:200:1000, GHRR_max, 1:200:1000, GHRS_max)
xlabel('Number of requests (1000)')
ylabel('Performance improvement compared to Simple Hybrid Service Placement')





figure

plot(2:2:1000, AHR1_perf(2:2:1000), 2:2:1000, AHRP1_perf(2:2:1000), 2:2:1000, AHRR1_perf(2:2:1000), 2:2:1000, AHRS1_perf(2:2:1000))
xlabel('Number of requests (1000)')
ylabel('Request Satisfaction Rate (%)')
legend('SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid')



figure

plot(2:2:1000, GHR1_perf(2:2:1000), 2:2:1000, GHRP1_perf(2:2:1000), 2:2:1000, GHRR1_perf(2:2:1000), 2:2:1000, GHRS1_perf(2:2:1000))
xlabel('Number of requests (1000)')
ylabel('Request Satisfaction Rate (%)')
legend('SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid')




figure

colormap(jet)
plot(1:2:1000, OH1_perf(2:2:1000), 2:2:1000, OG1_perf(2:2:1000), 2:2:1000, OR1_perf(2:2:1000), 2:2:1000, OS1_perf(2:2:1000), 1:2:1000, OAH1_perf(2:2:1000), '--', 2:2:1000, OAG1_perf(2:2:1000), '--', 2:2:1000, OAR1_perf(2:2:1000), '--', 2:2:1000, OAS1_perf(2:2:1000), '--')
xlabel('Number of requests (1000)')
ylabel('Normalized Overhead')
legend('Hybrid', 'SEND General-Purpose', 'SEND Function-Reactive', 'SEND Hybrid', 'Azure Hybrid', 'Azure SEND General-Purpose', 'Azure SEND Function-Reactive', 'Azure SEND Hybrid')


figure

semilogy(1:2:1000, OAH1_perf(2:2:1000), 2:2:1000, OAG1_perf(2:2:1000), 2:2:1000, OAP1_perf(2:2:1000), 2:2:1000, OAR1_perf(2:2:1000), 2:2:1000, OAS1_perf(2:2:1000))
xlabel('Number of requests (1000)')
ylabel('Request Satisfaction Rate (%)')
legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid')


figure

semilogy(1:2:1000, OGH1_perf(2:2:1000), 2:2:1000, OGG1_perf(2:2:1000), 2:2:1000, OGP1_perf(2:2:1000), 2:2:1000, OGR1_perf(2:2:1000), 2:2:1000, OGS1_perf(2:2:1000))
xlabel('Number of requests (1000)')
ylabel('Request Satisfaction Rate (%)')
legend('Hybrid', 'SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid')


% figure
% 
% y = polyval(fitted_AHR1, 2:2:1000); % your mean vector;
% x = 2:4:1000;
% std_dev = 1;
% AHR = AHR1_perf(2:2:1000);
% curve1 = AHR(1:2:500);
% curve2 = AHR(2:2:500);
% x2 = [x, fliplr(x)];
% inBetween = [curve1, fliplr(curve2)];
% fill(x2, inBetween, 'g');
% hold on;
% plot(2:2:1000, y, 'r', 'LineWidth', 2);

AH_overall_max = max(AH1_perf);
AHR_overall_max = max(AHR1_perf);
AHRP_overall_max = max(AHRP1_perf);
AHRR_overall_max = max(AHRR1_perf);
AHRS_overall_max = max(AHRS1_perf);

GH_overall_max = max(GH1_perf);
GHR_overall_max = max(GHR1_perf);
GHRP_overall_max = max(GHRP1_perf);
GHRR_overall_max = max(GHRR1_perf);
GHRS_overall_max = max(GHRS1_perf);



for i = 1:5

    OH_max(i) = max(OH1_perf(((i-1)*200+1):(i*200)));
    OH_avg(i) = mean(OH1_perf(((i-1)*200+1):(i*200)));
    OH_min(i) = min(OH1_perf(((i-1)*200+1):(i*200)));

end


for i = 1:5

    OG_max(i) = max(OG1_perf(((i-1)*200+1):(i*200)));
    OG_avg(i) = mean(OG1_perf(((i-1)*200+1):(i*200)));
    OG_min(i) = min(OG1_perf(((i-1)*200+1):(i*200)));

end


for i = 1:5

    OP_max(i) = max(OP1_perf(((i-1)*200+1):(i*200)));
    OP_avg(i) = mean(OP1_perf(((i-1)*200+1):(i*200)));
    OP_min(i) = min(OP1_perf(((i-1)*200+1):(i*200)));

end


for i = 1:5

    OR_max(i) = max(OR1_perf(((i-1)*200+1):(i*200)));
    OR_avg(i) = mean(OR1_perf(((i-1)*200+1):(i*200)));
    OR_min(i) = min(OR1_perf(((i-1)*200+1):(i*200)));

end


for i = 1:5

    OS_max(i) = max(OS1_perf(((i-1)*200+1):(i*200)));
    OS_avg(i) = mean(OS1_perf(((i-1)*200+1):(i*200)));
    OS_min(i) = min(OS1_perf(((i-1)*200+1):(i*200)));

end



for i = 1:5

    OAH_max(i) = max(OAH1_perf(((i-1)*200+1):(i*200)));
    OAH_avg(i) = mean(OAH1_perf(((i-1)*200+1):(i*200)));
    OAH_min(i) = min(OAH1_perf(((i-1)*200+1):(i*200)));

end


for i = 1:5

    OAG_max(i) = max(OAG1_perf(((i-1)*200+1):(i*200)));
    OAG_avg(i) = mean(OAG1_perf(((i-1)*200+1):(i*200)));
    OAG_min(i) = min(OAG1_perf(((i-1)*200+1):(i*200)));

end


for i = 1:5

    OAP_max(i) = max(OAP1_perf(((i-1)*200+1):(i*200)));
    OAP_avg(i) = mean(OAP1_perf(((i-1)*200+1):(i*200)));
    OAP_min(i) = min(OAP1_perf(((i-1)*200+1):(i*200)));

end


for i = 1:5

    OAR_max(i) = max(OAR1_perf(((i-1)*200+1):(i*200)));
    OAR_avg(i) = mean(OAR1_perf(((i-1)*200+1):(i*200)));
    OAR_min(i) = min(OAR1_perf(((i-1)*200+1):(i*200)));

end


for i = 1:5

    OAS_max(i) = max(OAS1_perf(((i-1)*200+1):(i*200)));
    OAS_avg(i) = mean(OAS1_perf(((i-1)*200+1):(i*200)));
    OAS_min(i) = min(OAS1_perf(((i-1)*200+1):(i*200)));

end





for i = 1:5

    OGH_max(i) = max(OGH1_perf(((i-1)*200+1):(i*200)));
    OGH_avg(i) = mean(OGH1_perf(((i-1)*200+1):(i*200)));
    OGH_min(i) = min(OGH1_perf(((i-1)*200+1):(i*200)));

end


for i = 1:5

    OGG_max(i) = max(OGG1_perf(((i-1)*200+1):(i*200)));
    OGG_avg(i) = mean(OGG1_perf(((i-1)*200+1):(i*200)));
    OGG_min(i) = min(OGG1_perf(((i-1)*200+1):(i*200)));

end


for i = 1:5

    OGP_max(i) = max(OGP1_perf(((i-1)*200+1):(i*200)));
    OGP_avg(i) = mean(OGP1_perf(((i-1)*200+1):(i*200)));
    OGP_min(i) = min(OGP1_perf(((i-1)*200+1):(i*200)));

end


for i = 1:5

    OGR_max(i) = max(OGR1_perf(((i-1)*200+1):(i*200)));
    OGR_avg(i) = mean(OGR1_perf(((i-1)*200+1):(i*200)));
    OGR_min(i) = min(OGR1_perf(((i-1)*200+1):(i*200)));

end


for i = 1:5

    OGS_max(i) = max(OGS1_perf(((i-1)*200+1):(i*200)));
    OGS_avg(i) = mean(OGS1_perf(((i-1)*200+1):(i*200)));
    OGS_min(i) = min(OGS1_perf(((i-1)*200+1):(i*200)));

end




figure
hold on

%subplot(1, 2, 1)
h = bar([1:200:1000], [OG_avg' + 6, OP_avg' + 6, OR_avg' + 6, OS_avg' + 6], 'group');
set(gca,'xtick',200:200:1200,'XTickLabel', 200:200:1200)
legend1 = legend('SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid');
xlabel('Number of requests (1000)')
ylabel('Average replications per service')
legend2 = legend('SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid');
% ax = gca();  
% % Set x tick to 1/2 way between bar groups
% ax.XTick = unique([h.XData]) + [diff(unique([h.XData]))/2, inf]; 
%subplot(1, 2, 2)
figure
g = bar([1:200:1000], [OAG_avg' + 6, OAP_avg' + 6, OAR_avg' + 6, OAS_avg' + 6], 'group');
set(gca,'xtick',200:200:1200,'XTickLabel', 200:200:1200)
% ax = gca();  
% % Set x tick to 1/2 way between bar groups
% ax.XTick = unique([.XData]) + [diff(unique([h.XData]))/2, inf]; 
% set(gca,'xtick',1:100:1100,'XTickLabel', 1:100:1100)
legend1 = legend('SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid');
% plot(1:200:1000, AH_min, 1:200:1000, AHR_min, 1:200:1000, AHRP_min, 1:200:1000, AHRR_min, 1:200:1000, AHRS_min)
% plot(1:200:1000, AH_max, 1:200:1000, AHR_max, 1:200:1000, AHRP_max, 1:200:1000, AHRR_max, 1:200:1000, AHRS_max)
xlabel('Number of requests (1000)')
ylabel('Average replications per service')
legend2 = legend('SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid');
%subplot(1, 2, 2)
figure
g = bar([1:200:1000], [OGG_avg' + 6, OGP_avg' + 6, OGR_avg' + 6, OGS_avg' + 6], 'group');
set(gca,'xtick',200:200:1200,'XTickLabel', 200:200:1200)
% ax = gca();  
% % Set x tick to 1/2 way between bar groups
% ax.XTick = unique([.XData]) + [diff(unique([h.XData]))/2, inf]; 
% set(gca,'xtick',1:100:1100,'XTickLabel', 1:100:1100)
legend1 = legend('SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid');
% plot(1:200:1000, GH_min, 1:200:1000, GHR_min, 1:200:1000, GHRP_min, 1:200:1000, GHRR_min, 1:200:1000, GHRS_min)
% plot(1:200:1000, GH_max, 1:200:1000, GHR_max, 1:200:1000, GHRP_max, 1:200:1000, GHRR_max, 1:200:1000, GHRS_max)
xlabel('Number of requests (1000)')
ylabel('Average replications per service')
legend2 = legend('SEND General-Purpose', 'SEND Popularity', 'SEND Function-Reactive', 'SEND Hybrid');
